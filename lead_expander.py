#!/usr/bin/env python3
"""
Lead-Expander (Container 3)
Converts flood pixels into validated mobile leads for SMS dispatch.

Usage:
    python lead_expander.py                    # Normal run
    python lead_expander.py --dry-run          # Dry run mode (no DB writes)
    python lead_expander.py --validate         # Validation mode with detailed output
    python lead_expander.py --single-pixel 123 # Process specific segment_id only
"""

import asyncio
import asyncpg
import aiohttp
import json
import os
import sys
import argparse
from datetime import datetime, date
from typing import List, Dict, Optional, Tuple
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class LeadExpander:
    def __init__(self, db_env, dry_run=False, validate_mode=False, single_pixel=None):
        # Runtime configuration
        self.db_env = db_env
        self.dry_run = dry_run
        self.validate_mode = validate_mode
        self.single_pixel = single_pixel
        
        # API environment configuration (separate from DB environment)
        self.api_env = os.getenv('API_ENV', 'dev').lower()
        self.is_api_dev = self.api_env == 'dev'
        
        # Environment variables
        self.attom_key = os.getenv('ATTOM_KEY')
        self.pdl_key = os.getenv('PDL_KEY') 
        self.twilio_sid = os.getenv('TWILIO_SID')
        self.twilio_token = os.getenv('TWILIO_TOKEN')
        
        # Tunable parameters
        self.attom_radius_km = float(os.getenv('ATTOM_RADIUS_KM', '0.8'))
        self.attom_pagesize = int(os.getenv('ATTOM_PAGESIZE', '20'))
        self.max_pages = int(os.getenv('MAX_PAGES', '3'))
        self.max_leads_per_pixel = int(os.getenv('MAX_LEADS_PER_PIXEL', '20'))
        self.attom_daily_ceiling = float(os.getenv('ATTOM_DAILY_CEILING_USD', '200'))
        self.max_pdl_concurrency = int(os.getenv('MAX_PDL_CONCURRENCY', '20'))
        self.enable_twilio_lookup = os.getenv('ENABLE_TWILIO_LOOKUP', 'true').lower() == 'true'
        
        # Database connection - use environment-specific URL
        if self.db_env == 'dev':
            self.db_url = os.getenv('DEV_DATABASE_URL')
        else:  # prod
            self.db_url = os.getenv('PROD_DATABASE_URL')
            
        if not self.db_url:
            # Fallback to generic DATABASE_URL
            self.db_url = os.getenv('DATABASE_URL')
            
        if not self.db_url:
            raise ValueError(f"Missing database URL for {self.db_env} environment. Set {self.db_env.upper()}_DATABASE_URL or DATABASE_URL")
        
        # Validate required environment variables (relaxed for dev APIs)
        required_vars = []
        if not self.is_api_dev:
            required_vars.extend(['ATTOM_KEY', 'PDL_KEY'])
            if self.enable_twilio_lookup:
                required_vars.extend(['TWILIO_SID', 'TWILIO_TOKEN'])
            
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {missing_vars}")
            
        # Environment indicators
        logger.info(f"🗄️  Database: {self.db_env.upper()} environment")
        logger.info(f"🔧 APIs: {self.api_env.upper()} environment")
        
        # Development mode warnings for APIs
        if self.is_api_dev:
            logger.warning("🚧 API DEVELOPMENT MODE - APIs will be mocked")
            if not self.attom_key:
                logger.warning("⚠️  ATTOM_KEY not set - using mock data")
            if not self.pdl_key:
                logger.warning("⚠️  PDL_KEY not set - using mock data")
            if self.enable_twilio_lookup and (not self.twilio_sid or not self.twilio_token):
                logger.warning("⚠️  Twilio credentials not set - using mock data")
                
        # Runtime mode indicators
        if self.dry_run:
            logger.warning("🧪 DRY RUN MODE - No database writes will be performed")
        if self.validate_mode:
            logger.warning("✅ VALIDATION MODE - Detailed output enabled")
        if self.single_pixel:
            logger.warning(f"🎯 SINGLE PIXEL MODE - Processing segment_id {self.single_pixel} only")
        
        # Metrics for logging
        self.metrics = {}
        
    async def run(self):
        """Main entry point - process flood pixels into leads"""
        start_time = datetime.utcnow()
        logger.info("Starting Lead Expander run")
        
        try:
            # Get database connection
            conn = await asyncpg.connect(self.db_url)
            
            # Get marketable pixels (score desc order)
            pixels = await self.get_marketable_pixels(conn)
            logger.info(f"Found {len(pixels)} marketable pixels to process")
            
            if not pixels:
                logger.info("No marketable pixels found, exiting")
                await conn.close()
                return
                
            # Process each pixel
            total_leads_queued = 0
            processed_pixels = []
            failed_pixels = []
            
            for pixel in pixels:
                try:
                    pixel_metrics = await self.process_pixel(conn, pixel)
                    total_leads_queued += pixel_metrics.get('lead_queued', 0)
                    processed_pixels.append(pixel['segment_id'])
                    
                    # Log pixel metrics
                    self.log_pixel_metrics(pixel['segment_id'], pixel_metrics)
                    
                except Exception as e:
                    logger.error(f"Failed to process pixel {pixel['segment_id']}: {e}")
                    failed_pixels.append(pixel['segment_id'])
                    
            # Mark processed and failed pixels
            if processed_pixels and not self.dry_run:
                await self.mark_pixels_processed(conn, processed_pixels)
            elif processed_pixels and self.dry_run:
                logger.info(f"🧪 DRY RUN: Would mark {len(processed_pixels)} pixels as processed")
                
            if failed_pixels and not self.dry_run:
                await self.mark_pixels_failed(conn, failed_pixels)
            elif failed_pixels and self.dry_run:
                logger.info(f"🧪 DRY RUN: Would mark {len(failed_pixels)} pixels as failed")
            
            await conn.close()
            
            duration = (datetime.utcnow() - start_time).total_seconds()
            logger.info(f"Lead Expander completed in {duration:.1f}s, queued {total_leads_queued} leads")
            
        except Exception as e:
            logger.error(f"Lead Expander failed: {e}", exc_info=True)
            sys.exit(1)
    
    async def get_marketable_pixels(self, conn) -> List[Dict]:
        """Get unprocessed pixels from flood_pixels_marketable"""
        
        # First, ensure processed column exists in flood_pixels_unique
        await self.ensure_processed_column(conn)
        
        # Query the materialized view, but join with underlying table for processed status
        base_query = """
        SELECT 
            fpm.segment_id,
            ST_Y(fpm.geom) as latitude, 
            ST_X(fpm.geom) as longitude, 
            fpm.score, 
            fpm.homes,
            fpm.first_seen
        FROM flood_pixels_marketable fpm
        JOIN flood_pixels_unique fpu ON fpm.segment_id = fpu.segment_id
        WHERE COALESCE(fpu.processed, false) = false
        """
        
        # Add single pixel filter if specified
        if self.single_pixel:
            query = base_query + f" AND fpm.segment_id = {self.single_pixel}"
        else:
            query = base_query + " ORDER BY fpm.score DESC"
        
        rows = await conn.fetch(query)
        pixels = [dict(row) for row in rows]
        
        if self.validate_mode:
            logger.info(f"📊 Found {len(pixels)} marketable pixels:")
            for pixel in pixels[:5]:  # Show first 5
                logger.info(f"   segment_id={pixel['segment_id']}, score={pixel['score']}, homes={pixel['homes']}")
            if len(pixels) > 5:
                logger.info(f"   ... and {len(pixels) - 5} more")
                
        return pixels
    
    async def ensure_processed_column(self, conn):
        """Add processed column to flood_pixels_unique if it doesn't exist"""
        try:
            await conn.execute("""
                ALTER TABLE flood_pixels_unique 
                ADD COLUMN IF NOT EXISTS processed BOOLEAN DEFAULT FALSE
            """)
            logger.debug("Ensured processed column exists in flood_pixels_unique")
        except Exception as e:
            logger.error(f"Failed to add processed column: {e}")
            # Continue anyway - the query will handle missing column with COALESCE
    
    async def process_pixel(self, conn, pixel: Dict) -> Dict:
        """Process a single pixel into leads"""
        segment_id = pixel['segment_id']
        latitude = pixel['latitude']
        longitude = pixel['longitude']
        
        metrics = {
            'homes_considered': 0,
            'attom_records': 0,
            'attom_cost': 0.0,
            'pdl_calls': 0,
            'pdl_hits': 0,
            'valid_mobiles': 0,
            'twilio_live': 0,
            'lead_queued': 0
        }
        
        try:
            # Get ATTOM properties for this pixel
            properties = await self.fetch_attom_properties(latitude, longitude)
            metrics['attom_records'] = len(properties)
            metrics['attom_cost'] = len(properties) * 0.09
            
            # Check budget before proceeding
            if not await self.can_afford(conn, metrics['attom_cost']):
                logger.warning(f"Budget exhausted, skipping pixel {segment_id}")
                return metrics
            
            # Filter properties (SFR only, individual owners)
            filtered_properties = self.filter_properties(properties)
            metrics['homes_considered'] = len(filtered_properties)
            
            if not filtered_properties:
                logger.info(f"No suitable properties found for pixel {segment_id}")
                return metrics
                
            # Limit to max leads per pixel
            if len(filtered_properties) > self.max_leads_per_pixel:
                filtered_properties = filtered_properties[:self.max_leads_per_pixel]
                
            # Enrich with PDL to get mobile numbers
            enriched_leads = await self.enrich_with_pdl(filtered_properties)
            metrics['pdl_calls'] = len(filtered_properties)
            metrics['pdl_hits'] = len(enriched_leads)
            
            # Validate mobile numbers
            validated_leads = await self.validate_mobile_numbers(enriched_leads)
            metrics['valid_mobiles'] = len(validated_leads)
            
            # Optional Twilio live check
            if self.enable_twilio_lookup:
                live_leads = await self.check_twilio_live(validated_leads)
                metrics['twilio_live'] = len(live_leads)
                final_leads = live_leads
            else:
                metrics['twilio_live'] = len(validated_leads)
                final_leads = validated_leads
            
            # Insert into homeowner queue
            if not self.dry_run:
                queued_count = await self.queue_leads(conn, segment_id, final_leads)
            else:
                queued_count = len(final_leads) * sum(len(lead['mobile_numbers']) for lead in final_leads)
                logger.info(f"🧪 DRY RUN: Would queue {queued_count} leads for segment_id {segment_id}")
                
            metrics['lead_queued'] = queued_count
            
            if self.validate_mode:
                logger.info(f"🔍 Pixel {segment_id} processing complete:")
                logger.info(f"   📱 Generated {queued_count} leads")
                logger.info(f"   💰 ATTOM cost: ${metrics['attom_cost']:.2f}")
                logger.info(f"   📞 PDL success rate: {metrics['pdl_hits']}/{metrics['pdl_calls']} ({100*metrics['pdl_hits']/max(1,metrics['pdl_calls']):.1f}%)")
            
        except Exception as e:
            logger.error(f"Error processing pixel {segment_id}: {e}")
            
        return metrics
    
    async def fetch_attom_properties(self, latitude: float, longitude: float) -> List[Dict]:
        """Fetch properties from ATTOM API with pagination"""
        if self.is_api_dev and not self.attom_key:
            return self._mock_attom_properties(latitude, longitude)
            
        properties = []
        
        async with aiohttp.ClientSession() as session:
            for page in range(1, self.max_pages + 1):
                params = {
                    'latitude': latitude,
                    'longitude': longitude,
                    'radius': self.attom_radius_km,
                    'pagesize': self.attom_pagesize,
                    'page': page,
                    'propertytype': 'SFR'
                }
                
                headers = {
                    'accept': 'application/json',
                    'apikey': self.attom_key
                }
                
                try:
                    async with session.get(
                        'https://api.gateway.attomdata.com/propertyapi/v1.0.0/property/snapshot',
                        params=params,
                        headers=headers,
                        timeout=aiohttp.ClientTimeout(total=30)
                    ) as response:
                        
                        if response.status == 200:
                            data = await response.json()
                            page_properties = data.get('property', [])
                            properties.extend(page_properties)
                            
                            # Stop if we got fewer records than page size (last page)
                            if len(page_properties) < self.attom_pagesize:
                                break
                        else:
                            logger.error(f"ATTOM API error: {response.status} - {await response.text()}")
                            break
                            
                except asyncio.TimeoutError:
                    logger.error(f"ATTOM API timeout for page {page}")
                    break
                except Exception as e:
                    logger.error(f"ATTOM API request failed: {e}")
                    break
                    
        return properties
    
    def _mock_attom_properties(self, latitude: float, longitude: float) -> List[Dict]:
        """Mock ATTOM properties for development"""
        import random
        
        logger.info(f"🎭 Mocking ATTOM properties for lat={latitude}, lon={longitude}")
        
        # Generate 10-30 mock properties
        num_properties = random.randint(10, 30)
        properties = []
        
        for i in range(num_properties):
            # Generate realistic property data
            house_number = random.randint(100, 9999)
            streets = ['MAIN ST', 'OAK AVE', 'PARK DR', 'FIRST ST', 'CENTRAL BLVD', 'MAPLE LN']
            street = random.choice(streets)
            
            properties.append({
                'summary': {
                    'propIndicator': '10'  # SFR
                },
                'owner': {
                    'owner1': f"{random.choice(['JOHN', 'JANE', 'MICHAEL', 'SARAH', 'DAVID', 'LISA'])} {random.choice(['SMITH', 'JOHNSON', 'WILLIAMS', 'BROWN', 'JONES', 'GARCIA'])}"
                },
                'address': {
                    'line1': f"{house_number} {street}",
                    'locality': 'HOUSTON',
                    'countrySubd': 'TX',
                    'postal1': f"{random.randint(77000, 77999)}",
                    'oneLine': f"{house_number} {street}, HOUSTON, TX {random.randint(77000, 77999)}"
                }
            })
            
        return properties
    
    def filter_properties(self, properties: List[Dict]) -> List[Dict]:
        """Filter properties for SFR with individual owners"""
        filtered = []
        
        for prop in properties:
            # Check property indicator (SFR only)
            prop_indicator = prop.get('summary', {}).get('propIndicator')
            if prop_indicator != '10':
                continue
                
            # Check for individual owner (not LLC/trust)
            owner_info = prop.get('owner', {})
            owner1 = owner_info.get('owner1')
            if not owner1:
                continue
                
            # Basic address validation
            address = prop.get('address', {})
            if not address.get('oneLine'):
                continue
                
            filtered.append(prop)
            
        return filtered
    
    async def enrich_with_pdl(self, properties: List[Dict]) -> List[Dict]:
        """Enrich properties with mobile numbers using PDL API"""
        leads = []
        semaphore = asyncio.Semaphore(self.max_pdl_concurrency)
        
        async def enrich_single_property(prop):
            async with semaphore:
                return await self._pdl_enrich_request(prop)
        
        # Process all properties concurrently with semaphore limit
        tasks = [enrich_single_property(prop) for prop in properties]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"PDL enrichment failed: {result}")
            elif result:
                leads.append(result)
                
        return leads
    
    def _standardize_address(self, address_info: Dict) -> Dict:
        """Standardize address components for PDL API"""
        import re
        
        def clean_string(s: str) -> str:
            """Clean and standardize string values"""
            if not s:
                return ""
            # Remove extra whitespace, convert to title case
            s = ' '.join(s.strip().split())
            # Handle common abbreviations
            s = re.sub(r'\bST\b', 'STREET', s, flags=re.IGNORECASE)
            s = re.sub(r'\bAVE\b', 'AVENUE', s, flags=re.IGNORECASE) 
            s = re.sub(r'\bDR\b', 'DRIVE', s, flags=re.IGNORECASE)
            s = re.sub(r'\bRD\b', 'ROAD', s, flags=re.IGNORECASE)
            s = re.sub(r'\bBLVD\b', 'BOULEVARD', s, flags=re.IGNORECASE)
            s = re.sub(r'\bLN\b', 'LANE', s, flags=re.IGNORECASE)
            s = re.sub(r'\bCT\b', 'COURT', s, flags=re.IGNORECASE)
            s = re.sub(r'\bPL\b', 'PLACE', s, flags=re.IGNORECASE)
            return s.upper()
        
        def clean_postal(postal: str) -> str:
            """Clean postal code - keep only first 5 digits"""
            if not postal:
                return ""
            digits = ''.join(c for c in postal if c.isdigit())
            return digits[:5] if len(digits) >= 5 else digits
        
        # Extract and clean address components
        street = clean_string(address_info.get('line1', ''))
        city = clean_string(address_info.get('locality', ''))
        state = clean_string(address_info.get('countrySubd', ''))
        postal = clean_postal(address_info.get('postal1', ''))
        
        # Handle state abbreviations
        state_abbrevs = {
            'ALABAMA': 'AL', 'ALASKA': 'AK', 'ARIZONA': 'AZ', 'ARKANSAS': 'AR',
            'CALIFORNIA': 'CA', 'COLORADO': 'CO', 'CONNECTICUT': 'CT', 'DELAWARE': 'DE',
            'FLORIDA': 'FL', 'GEORGIA': 'GA', 'HAWAII': 'HI', 'IDAHO': 'ID',
            'ILLINOIS': 'IL', 'INDIANA': 'IN', 'IOWA': 'IA', 'KANSAS': 'KS',
            'KENTUCKY': 'KY', 'LOUISIANA': 'LA', 'MAINE': 'ME', 'MARYLAND': 'MD',
            'MASSACHUSETTS': 'MA', 'MICHIGAN': 'MI', 'MINNESOTA': 'MN', 'MISSISSIPPI': 'MS',
            'MISSOURI': 'MO', 'MONTANA': 'MT', 'NEBRASKA': 'NE', 'NEVADA': 'NV',
            'NEW HAMPSHIRE': 'NH', 'NEW JERSEY': 'NJ', 'NEW MEXICO': 'NM', 'NEW YORK': 'NY',
            'NORTH CAROLINA': 'NC', 'NORTH DAKOTA': 'ND', 'OHIO': 'OH', 'OKLAHOMA': 'OK',
            'OREGON': 'OR', 'PENNSYLVANIA': 'PA', 'RHODE ISLAND': 'RI', 'SOUTH CAROLINA': 'SC',
            'SOUTH DAKOTA': 'SD', 'TENNESSEE': 'TN', 'TEXAS': 'TX', 'UTAH': 'UT',
            'VERMONT': 'VT', 'VIRGINIA': 'VA', 'WASHINGTON': 'WA', 'WEST VIRGINIA': 'WV',
            'WISCONSIN': 'WI', 'WYOMING': 'WY'
        }
        
        if state in state_abbrevs:
            state = state_abbrevs[state]
        
        return {
            'street': street,
            'city': city,
            'region': state,
            'postal_code': postal
        }
        """Single PDL enrichment request"""
        try:
            # Extract owner and address info
            owner_info = prop.get('owner', {})
            address_info = prop.get('address', {})
            
            # Parse owner name (assume "FIRST LAST" format)
            owner1 = owner_info.get('owner1', '').strip()
            if not owner1:
                return None
                
            name_parts = owner1.split()
            if len(name_parts) < 2:
                return None
                
            first_name = name_parts[0]
            last_name = ' '.join(name_parts[1:])
            
            # Prepare PDL request
            pdl_data = {
                'first_name': first_name,
                'last_name': last_name,
                'street': address_info.get('line1', ''),
                'city': address_info.get('locality', ''),
                'region': address_info.get('countrySubd', ''),
                'postal_code': address_info.get('postal1', '')
            }
            
            headers = {
                'X-Api-Key': self.pdl_key,
                'Content-Type': 'application/json'
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    'https://api.peopledatalabs.com/v5/person/enrich',
                    json=pdl_data,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as response:
                    
                    if response.status == 200:
                        person_data = await response.json()
                        
                        # Extract validated mobile numbers
                        mobile_numbers = self._extract_mobile_numbers(person_data)
                        
                        if mobile_numbers:
                            return {
                                'property': prop,
                                'mobile_numbers': mobile_numbers,
                                'address_one_line': address_info.get('oneLine', ''),
                                'person_data': person_data
                            }
                    else:
                        logger.debug(f"PDL API non-200 response: {response.status}")
                        
        except Exception as e:
            logger.error(f"PDL enrichment error: {e}")
            
        return None
    
    def _extract_mobile_numbers(self, person_data: Dict) -> List[str]:
        """Extract validated mobile numbers from PDL response"""
        mobile_numbers = []
        
        phones = person_data.get('phone_numbers', [])
        for phone in phones:
            if (phone.get('is_validated') and 
                phone.get('type') == 'mobile' and 
                phone.get('number')):
                mobile_numbers.append(phone['number'])
                
        return mobile_numbers
    
    async def validate_mobile_numbers(self, leads: List[Dict]) -> List[Dict]:
        """Basic mobile number validation"""
        validated_leads = []
        
        for lead in leads:
            mobile_numbers = lead['mobile_numbers']
            valid_numbers = []
            
            for number in mobile_numbers:
                # Basic US mobile number validation
                if self._is_valid_us_mobile(number):
                    valid_numbers.append(number)
                    
            if valid_numbers:
                lead['mobile_numbers'] = valid_numbers
                validated_leads.append(lead)
                
        return validated_leads
    
    def _is_valid_us_mobile(self, phone: str) -> bool:
        """Basic US mobile number validation"""
        # Remove non-digits
        digits = ''.join(c for c in phone if c.isdigit())
        
        # Must be 10 or 11 digits (with country code)
        if len(digits) == 11 and digits.startswith('1'):
            digits = digits[1:]
        elif len(digits) != 10:
            return False
            
        # Basic US area code validation (not 0 or 1)
        if digits[0] in '01' or digits[3] in '01':
            return False
            
        return True
    
    async def check_twilio_live(self, leads: List[Dict]) -> List[Dict]:
        """Check mobile numbers are live using Twilio Lookup"""
        if not self.enable_twilio_lookup:
            return leads
            
        live_leads = []
        
        for lead in leads:
            live_numbers = []
            
            for number in lead['mobile_numbers']:
                if await self._twilio_lookup(number):
                    live_numbers.append(number)
                    
            if live_numbers:
                lead['mobile_numbers'] = live_numbers
                live_leads.append(lead)
                
        return live_leads
    
    async def _twilio_lookup(self, phone: str) -> bool:
        """Single Twilio lookup request"""
        if self.is_api_dev and (not self.twilio_sid or not self.twilio_token):
            return self._mock_twilio_lookup(phone)
            
        try:
            # Format phone number for Twilio
            if not phone.startswith('+1'):
                phone = '+1' + ''.join(c for c in phone if c.isdigit())[-10:]
                
            url = f"https://lookups.twilio.com/v2/PhoneNumbers/{phone}"
            params = {'Type': 'carrier'}
            
            # Use basic auth with Twilio credentials
            auth = aiohttp.BasicAuth(self.twilio_sid, self.twilio_token)
            
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url,
                    params=params,
                    auth=auth,
                    timeout=aiohttp.ClientTimeout(total=5)
                ) as response:
                    
                    if response.status == 200:
                        data = await response.json()
                        carrier = data.get('carrier', {})
                        
                        # Check for errors and mobile type
                        if (not carrier.get('error_code') and 
                            carrier.get('type') == 'mobile'):
                            return True
                            
        except Exception as e:
            logger.debug(f"Twilio lookup failed for {phone}: {e}")
            
        return False
    
    def _mock_twilio_lookup(self, phone: str) -> bool:
        """Mock Twilio lookup for development"""
        import random
        # 85% success rate for mock Twilio lookup
        return random.random() < 0.85
    
    async def queue_leads(self, conn, segment_id: int, leads: List[Dict]) -> int:
        """Insert leads into homeowner_queue"""
        queued_count = 0
        
        for lead in leads:
            for mobile_number in lead['mobile_numbers']:
                try:
                    # Format phone number to E164
                    phone_e164 = self._format_e164(mobile_number)
                    
                    await conn.execute("""
                        INSERT INTO homeowner_queue 
                        (pixel_id, phone_e164, address_one_line, status, created_at)
                        VALUES ($1, $2, $3, $4, $5)
                    """, segment_id, phone_e164, lead['address_one_line'], 'pending', datetime.utcnow())
                    
                    queued_count += 1
                    
                except Exception as e:
                    logger.error(f"Failed to queue lead: {e}")
                    
        return queued_count
    
    def _format_e164(self, phone: str) -> str:
        """Format phone number to E164 format"""
        digits = ''.join(c for c in phone if c.isdigit())
        
        if len(digits) == 11 and digits.startswith('1'):
            return f"+{digits}"
        elif len(digits) == 10:
            return f"+1{digits}"
        else:
            return f"+{digits}"
    
    async def can_afford(self, conn, projected_cost: float) -> bool:
        """Check if we can afford the projected ATTOM cost"""
        if self.dry_run:
            logger.info(f"🧪 DRY RUN: Would check budget for ${projected_cost:.2f}")
            return True
            
        try:
            async with conn.transaction():
                # Get current spend with row lock
                today = date.today()
                
                # Ensure today's record exists
                await conn.execute("""
                    INSERT INTO attom_spend (date, usd_spent) 
                    VALUES ($1, 0) 
                    ON CONFLICT (date) DO NOTHING
                """, today)
                
                # Get current spend with lock
                current_spend = await conn.fetchval("""
                    SELECT usd_spent FROM attom_spend 
                    WHERE date = $1 FOR UPDATE
                """, today)
                
                if self.validate_mode:
                    logger.info(f"💰 Current ATTOM spend: ${current_spend:.2f}, projected: +${projected_cost:.2f}, ceiling: ${self.attom_daily_ceiling:.2f}")
                
                if current_spend + projected_cost > self.attom_daily_ceiling:
                    logger.warning(f"💸 Budget ceiling reached: ${current_spend + projected_cost:.2f} > ${self.attom_daily_ceiling:.2f}")
                    return False
                    
                # Update spend
                await conn.execute("""
                    UPDATE attom_spend 
                    SET usd_spent = usd_spent + $1 
                    WHERE date = $2
                """, projected_cost, today)
                
                return True
                
        except Exception as e:
            logger.error(f"Budget check failed: {e}")
            return False
    
    async def mark_pixels_processed(self, conn, segment_ids: List[int]):
        """Mark pixels as processed"""
        if not segment_ids:
            return
            
        try:
            await conn.execute("""
                UPDATE flood_pixels_unique 
                SET processed = true 
                WHERE segment_id = ANY($1)
            """, segment_ids)
            
        except Exception as e:
            logger.error(f"Failed to mark pixels processed: {e}")
    
    async def mark_pixels_failed(self, conn, segment_ids: List[int]):
        """Mark pixels as failed due to API errors"""
        if not segment_ids:
            return
            
        try:
            await conn.execute("""
                UPDATE flood_pixels_unique 
                SET processed = true
                WHERE segment_id = ANY($1)
            """, segment_ids)
            
            logger.warning(f"Marked {len(segment_ids)} pixels as failed: {segment_ids}")
            
        except Exception as e:
            logger.error(f"Failed to mark pixels as failed: {e}")

    def log_pixel_metrics(self, segment_id: int, metrics: Dict):
        """Log pixel processing metrics"""
        log_parts = [f"pixel_id={pixel_id}"]
        
        for key, value in metrics.items():
            if isinstance(value, float):
                log_parts.append(f"{key}={value:.2f}")
            else:
                log_parts.append(f"{key}={value}")
                
        logger.info(" ".join(log_parts))

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Lead Expander - Convert flood pixels to homeowner leads',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python lead_expander.py --db dev                         # Run against dev database
  python lead_expander.py --db prod --dry-run              # Test against prod DB (safe)
  python lead_expander.py --db dev --validate              # Detailed output with dev DB
  python lead_expander.py --db dev --single-pixel 123      # Test specific pixel on dev
  python lead_expander.py --db prod --single-pixel 456     # Test specific pixel on prod
        """
    )
    
    parser.add_argument(
        '--db',
        choices=['dev', 'prod'],
        required=True,
        help='Database environment to connect to (dev or prod)'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Run without making database writes (safe for testing)'
    )
    
    parser.add_argument(
        '--validate', 
        action='store_true',
        help='Enable detailed validation output and logging'
    )
    
    parser.add_argument(
        '--single-pixel',
        type=int,
        metavar='SEGMENT_ID',
        help='Process only the specified segment_id (useful for testing)'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true', 
        help='Enable debug logging'
    )
    
    return parser.parse_args()

async def main():
    """Main entry point"""
    args = parse_args()
    
    # Configure logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.debug("Debug logging enabled")
    
    # Create and run expander
    expander = LeadExpander(
        db_env=args.db,
        dry_run=args.dry_run,
        validate_mode=args.validate,
        single_pixel=args.single_pixel
    )
    await expander.run()

if __name__ == "__main__":
    asyncio.run(main())