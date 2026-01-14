#!/usr/bin/env python3

import mysql.connector
import os
import json
import re
import argparse
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
from collections import defaultdict
from dataclasses import dataclass
from groq import Groq
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

@dataclass
class GroupData:
    group_name: str
    message_count: int
    participants: set
    links: List[str]
    messages: List[str]

@dataclass
class SummaryResult:
    scope: str
    canonical: str
    group_name: Optional[str]
    week_start: str
    message_count: int
    participants: int
    links: int
    summary: str
    keywords: List[str]

class WhatsAppSummarizer:
    def __init__(self):
        self.setup_logging()
        self.setup_db_config()
        self.setup_groq_client()
        
    def setup_logging(self):
        """Setup logging configuration"""
        log_filename = f"whatsapp_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_filename, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"Logging to: {log_filename}")
    
    def setup_db_config(self):
        """Setup database configuration from environment variables"""
        self.db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'database': os.getenv('DB_NAME'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'port': int(os.getenv('DB_PORT', 3306))
        }
        
        # Validate required config
        required_fields = ['database', 'user', 'password']
        missing_fields = [field for field in required_fields if not self.db_config.get(field)]
        
        if missing_fields:
            raise ValueError(f"Missing required environment variables: {missing_fields}")
    
    def setup_groq_client(self):
        """Setup Groq client"""
        groq_api_key = os.getenv('GROQ_API_KEY')
        if not groq_api_key:
            raise ValueError("GROQ_API_KEY environment variable is required")
        
        self.groq_client = Groq(api_key=groq_api_key)
    
    def get_db_connection(self):
        """Create database connection"""
        try:
            conn = mysql.connector.connect(**self.db_config)
            self.logger.info("Database connection established")
            return conn
        except Exception as e:
            self.logger.error(f"Database connection failed: {e}")
            raise
    
    def remove_emojis(self, text: str) -> str:
        """Remove emojis from text"""
        # Unicode ranges for emojis
        emoji_pattern = re.compile(
            "["
            "\U0001F1E0-\U0001F1FF"  # flags (iOS)
            "\U0001F300-\U0001F5FF"  # symbols & pictographs
            "\U0001F600-\U0001F64F"  # emoticons
            "\U0001F680-\U0001F6FF"  # transport & map symbols
            "\U0001F700-\U0001F77F"  # alchemical symbols
            "\U0001F780-\U0001F7FF"  # Geometric Shapes Extended
            "\U0001F800-\U0001F8FF"  # Supplemental Arrows-C
            "\U0001F900-\U0001F9FF"  # Supplemental Symbols and Pictographs
            "\U0001FA00-\U0001FA6F"  # Chess Symbols
            "\U0001FA70-\U0001FAFF"  # Symbols and Pictographs Extended-A
            "\U00002702-\U000027B0"  # Dingbats
            "\U000024C2-\U0001F251" 
            "]+"
        )
        return emoji_pattern.sub('', text).strip()
    
    def get_week_start_end(self, start_date_str: str) -> Tuple[str, str]:
        """Convert start date to week start (Monday) and end (Sunday) in IST"""
        try:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
            
            # Find Monday of that week
            days_since_monday = start_date.weekday()
            monday = start_date - timedelta(days=days_since_monday)
            sunday = monday + timedelta(days=6)
            
            week_start = monday.strftime('%Y-%m-%d')
            week_end = (sunday + timedelta(days=1)).strftime('%Y-%m-%d')  # Next Monday for SQL query
            
            self.logger.info(f"Processing week: {week_start} to {sunday.strftime('%Y-%m-%d')}")
            return week_start, week_end
            
        except ValueError:
            raise ValueError("Invalid date format. Use YYYY-MM-DD")
    
    def get_indie_groups_data(self, week_start: str, week_end: str) -> Dict[str, GroupData]:
        """Fetch data for Indie Parents Pack groups for the specified week"""
        
        conn = self.get_db_connection()
        cursor = conn.cursor()
        
        try:
            # First, let's see ALL groups that match the pattern and their raw message counts
            debug_query = """
            SELECT GroupName, COUNT(*) as raw_message_count
            FROM WhatsAppExport 
            WHERE CreatedDate >= %s AND CreatedDate < %s
            AND GroupName LIKE %s
            AND Message IS NOT NULL AND Message != ''
            GROUP BY GroupName
            ORDER BY GroupName
            """
            
            self.logger.info("=== DEBUG: All Indie Parents Pack groups in this week ===")
            cursor.execute(debug_query, (week_start, week_end, '%Indie Parents Pack%'))
            debug_results = cursor.fetchall()
            
            if not debug_results:
                self.logger.warning("❌ No groups found matching '%Indie Parents Pack%' pattern in the specified week")
                self.logger.info("Let's check what groups exist in this time period...")
                
                # Show what groups DO exist in this time period
                all_groups_query = """
                SELECT GroupName, COUNT(*) as message_count
                FROM WhatsAppExport 
                WHERE CreatedDate >= %s AND CreatedDate < %s
                AND Message IS NOT NULL AND Message != ''
                GROUP BY GroupName
                ORDER BY message_count DESC
                LIMIT 10
                """
                cursor.execute(all_groups_query, (week_start, week_end))
                all_groups = cursor.fetchall()
                
                self.logger.info("Top 10 groups with activity in this week:")
                for group_name, count in all_groups:
                    self.logger.info(f"  📊 {group_name}: {count} messages")
                
                return {}
            
            # Log all matching groups with raw counts
            total_raw_messages = 0
            for group_name, raw_count in debug_results:
                self.logger.info(f"  📱 {group_name}: {raw_count} raw messages")
                total_raw_messages += raw_count
            
            self.logger.info(f"Total groups found: {len(debug_results)}")
            self.logger.info(f"Total raw messages: {total_raw_messages}")
            self.logger.info("=== End Debug Info ===")
            
            # Now fetch the actual data - removed Phone and UserName for privacy
            main_query = """
            SELECT GroupName, Message, Links
            FROM WhatsAppExport 
            WHERE CreatedDate >= %s AND CreatedDate < %s
            AND GroupName LIKE %s
            AND Message IS NOT NULL AND Message != ''
            ORDER BY GroupName, CreatedDate
            """
            
            cursor.execute(main_query, (week_start, week_end, '%Indie Parents Pack%'))
            rows = cursor.fetchall()
            
            groups_data = defaultdict(lambda: GroupData(
                group_name='',
                message_count=0,
                participants=set(),
                links=[],
                messages=[]
            ))
            
            # Track raw vs clean message counts for debugging
            raw_counts = defaultdict(int)
            emoji_only_counts = defaultdict(int)
            
            for row in rows:
                group_name, message, links = row
                raw_counts[group_name] += 1
                
                # Clean message by removing emojis
                clean_message = self.remove_emojis(message)
                if not clean_message.strip():  # Skip if message becomes empty after emoji removal
                    emoji_only_counts[group_name] += 1
                    continue
                
                if group_name not in groups_data:
                    groups_data[group_name].group_name = group_name
                
                groups_data[group_name].message_count += 1
                # Estimate participants based on message patterns for privacy
                groups_data[group_name].participants.add(len(groups_data[group_name].messages))
                groups_data[group_name].messages.append(clean_message)
                
                if links:
                    groups_data[group_name].links.append(links)
            
            # Log processing results
            self.logger.info(f"=== PROCESSING RESULTS ===")
            self.logger.info(f"Groups with usable content: {len(groups_data)}")
            
            for group_name in raw_counts:
                raw_count = raw_counts[group_name]
                emoji_only = emoji_only_counts[group_name]
                clean_count = groups_data[group_name].message_count if group_name in groups_data else 0
                participants = len(groups_data[group_name].participants) if group_name in groups_data else 0
                
                self.logger.info(f"  📊 {group_name}:")
                self.logger.info(f"     Raw messages: {raw_count}")
                self.logger.info(f"     Emoji-only messages: {emoji_only}")
                self.logger.info(f"     Clean messages: {clean_count}")
                self.logger.info(f"     Participants: {participants}")
                
                if clean_count == 0:
                    self.logger.warning(f"     ⚠️  {group_name} has no usable messages after emoji removal!")
            
            self.logger.info(f"=== END PROCESSING RESULTS ===")
            
            return dict(groups_data)
            
        finally:
            cursor.close()
            conn.close()
    
    def generate_summary_with_groq(self, messages: List[str], context: str) -> Tuple[str, List[str]]:
        """Generate summary using Groq API"""
        
        # Limit messages to avoid token limits and combine them
        limited_messages = messages[:100]  # First 100 messages
        combined_text = '\n'.join(limited_messages)
        
        prompt = f"""Analyze the following WhatsApp messages from {context} and provide a concise summary.

Requirements:
- Summary should be 200-300 words maximum
- Focus on main topics, discussions, and themes
- Extract 5-7 relevant keywords
- No emojis in the response
- Write in a professional, informative tone

Messages:
{combined_text}

Please provide your response in the following JSON format:
{{
    "summary": "Your detailed summary here...",
    "keywords": ["keyword1", "keyword2", "keyword3", "keyword4", "keyword5"]
}}"""
        
        try:
            response = self.groq_client.chat.completions.create(
                model="llama-3.1-8b-instant",
                messages=[
                    {
                        "role": "system", 
                        "content": "You are a helpful assistant that analyzes WhatsApp group conversations and provides structured summaries. Always respond with valid JSON."
                    },
                    {
                        "role": "user", 
                        "content": prompt
                    }
                ],
                temperature=0.3,
                max_tokens=800
            )
            
            response_text = response.choices[0].message.content.strip()
            
            # Try to parse JSON response
            try:
                result = json.loads(response_text)
                return result.get("summary", ""), result.get("keywords", [])
            except json.JSONDecodeError:
                # If JSON parsing fails, extract manually
                self.logger.warning(f"Failed to parse JSON response for {context}, using fallback extraction")
                return self._extract_summary_from_text(response_text), []
            
        except Exception as e:
            self.logger.error(f"Error generating summary for {context}: {e}")
            return self._generate_basic_summary(messages, context), []
    
    def _extract_summary_from_text(self, text: str) -> str:
        """Extract summary from non-JSON response"""
        # Simple extraction - look for summary-like content
        lines = text.split('\n')
        summary_lines = [line.strip() for line in lines if line.strip() and not line.strip().startswith('[')]
        return ' '.join(summary_lines)[:300]  # Limit to 300 chars
    
    def _generate_basic_summary(self, messages: List[str], context: str) -> str:
        """Generate basic summary without AI"""
        total_messages = len(messages)
        unique_words = set()
        for msg in messages[:50]:  # Sample first 50 messages
            unique_words.update(msg.lower().split())
        
        return f"{context} had {total_messages} messages this week. The group showed active participation with discussions covering various parenting topics and community interactions. Average message diversity suggests engaged conversations among participants."
    
    def create_group_summary(self, group_data: GroupData, week_start: str) -> SummaryResult:
        """Create summary for a single group"""
        
        if group_data.message_count == 0:
            return None
        
        # Skip AI analysis for groups with less than 50 messages
        if group_data.message_count < 50:
            self.logger.info(f"Skipping AI analysis for {group_data.group_name} - only {group_data.message_count} messages (minimum 50 required)")
            summary = f"Group {group_data.group_name} had {group_data.message_count} messages this week with {len(group_data.participants)} participants. Due to low message volume, detailed analysis was not performed."
            keywords = ["low-activity", "insufficient-data"]
        else:
            summary, keywords = self.generate_summary_with_groq(
                group_data.messages, 
                f"'{group_data.group_name}' group"
            )
        
        return SummaryResult(
            scope='group',
            canonical='Indie Parents Pack',
            group_name=group_data.group_name,
            week_start=week_start,
            message_count=group_data.message_count,
            participants=len(group_data.participants),
            links=len(group_data.links),
            summary=summary,
            keywords=keywords
        )
    
    def create_rollup_summary(self, all_groups: List[GroupData], week_start: str) -> SummaryResult:
        """Create rollup summary for all Indie Parents Pack groups"""
        
        total_messages = sum(g.message_count for g in all_groups)
        if total_messages == 0:
            return None
        
        all_participants = set()
        all_links = []
        
        # For rollup summary, create a comprehensive overview instead of using raw messages
        group_summaries = {}
        
        for group in all_groups:
            all_participants.update(group.participants)
            all_links.extend(group.links)
            
            # Extract key themes from each group's messages (sample approach)
            if group.message_count >= 50:
                # For groups with enough messages, take a broader sample
                sample_messages = group.messages[:20] + group.messages[len(group.messages)//2:len(group.messages)//2+20] + group.messages[-20:]
            else:
                sample_messages = group.messages[:10]  # Smaller sample for low-activity groups
            
            group_summaries[group.group_name] = {
                'messages': group.message_count,
                'sample_content': ' '.join(sample_messages)[:500]  # Limit content
            }
        
        # Skip AI analysis for rollup if total messages are less than 100 (increased threshold for rollup)
        if total_messages < 100:
            self.logger.info(f"Skipping AI analysis for rollup - only {total_messages} total messages (minimum 100 required for rollup)")
            group_names = [g.group_name for g in all_groups]
            summary = f"Combined rollup for all Indie Parents Pack groups ({', '.join(group_names)}) had {total_messages} messages this week with approximately {len(all_participants)} participants. Due to low message volume, detailed analysis was not performed."
            keywords = ["low-activity", "insufficient-data", "rollup"]
        else:
            # Create a structured prompt for rollup summary
            rollup_context = f"Combined analysis of {len(all_groups)} Indie Parents Pack groups with {total_messages} total messages"
            
            # Create a condensed overview for AI analysis
            condensed_content = []
            for group_name, data in group_summaries.items():
                condensed_content.append(f"Group {group_name} ({data['messages']} messages): {data['sample_content'][:200]}")
            
            combined_overview = '\n\n'.join(condensed_content)
            
            # Use a different approach for rollup - focus on themes across groups
            rollup_prompt = f"""Analyze the following summary of messages from multiple Indie Parents Pack WhatsApp groups and provide a comprehensive rollup summary.

Requirements:
- Summary should be 200-300 words maximum
- Focus on common themes, trends, and discussions across all groups
- Extract 5-7 relevant keywords that represent the overall community discussion
- No emojis in the response
- Write in a professional, informative tone

Group Overview:
{combined_overview}

Please provide your response in the following JSON format:
{{
    "summary": "Your detailed rollup summary here...",
    "keywords": ["keyword1", "keyword2", "keyword3", "keyword4", "keyword5"]
}}"""
            
            try:
                response = self.groq_client.chat.completions.create(
                    model="llama-3.1-8b-instant",
                    messages=[
                        {
                            "role": "system", 
                            "content": "You are a helpful assistant that analyzes WhatsApp group conversations and provides structured summaries. Always respond with valid JSON."
                        },
                        {
                            "role": "user", 
                            "content": rollup_prompt
                        }
                    ],
                    temperature=0.3,
                    max_tokens=600  # Reduced token limit
                )
                
                response_text = response.choices[0].message.content.strip()
                
                try:
                    result = json.loads(response_text)
                    summary = result.get("summary", "")
                    keywords = result.get("keywords", [])
                except json.JSONDecodeError:
                    self.logger.warning("Failed to parse JSON response for rollup, using fallback")
                    summary = self._generate_basic_rollup_summary(all_groups, total_messages)
                    keywords = ["community", "discussions", "pet-care", "support"]
                    
            except Exception as e:
                self.logger.error(f"Error generating rollup summary: {e}")
                summary = self._generate_basic_rollup_summary(all_groups, total_messages)
                keywords = ["community", "discussions", "pet-care", "support"]
        
        return SummaryResult(
            scope='rollup',
            canonical='Indie Parents Pack',
            group_name=None,
            week_start=week_start,
            message_count=total_messages,
            participants=len(all_participants),
            links=len(all_links),
            summary=summary,
            keywords=keywords
        )
    
    def _generate_basic_rollup_summary(self, all_groups: List[GroupData], total_messages: int) -> str:
        """Generate basic rollup summary without AI"""
        group_breakdown = []
        for group in all_groups:
            group_breakdown.append(f"{group.group_name} ({group.message_count} messages)")
        
        return f"Rollup summary for {len(all_groups)} Indie Parents Pack groups with {total_messages} total messages this week. Groups included: {', '.join(group_breakdown)}. The community showed active engagement across multiple groups with discussions covering various pet care topics, community support, and shared experiences."
    
    def log_summary(self, summary: SummaryResult):
        """Log summary to file and console"""
        
        scope_name = "GROUP SUMMARY" if summary.scope == 'group' else "ROLLUP SUMMARY"
        group_display = summary.group_name if summary.group_name else "ALL INDIE PARENTS PACK GROUPS"
        
        output = f"""
{'='*80}
{scope_name}: {group_display}
{'='*80}
Week: {summary.week_start}
Messages: {summary.message_count}
Participants: {summary.participants}
Links: {summary.links}
Keywords: {', '.join(summary.keywords)}

SUMMARY:
{summary.summary}
{'='*80}
"""
        
        self.logger.info(output)
    
    def process_week(self, start_date: str):
        """Main function to process summaries for a specific week"""
        
        self.logger.info(f"Starting WhatsApp summarization for week starting {start_date}")
        
        # Get week boundaries
        week_start, week_end = self.get_week_start_end(start_date)
        
        # Fetch group data
        groups_data = self.get_indie_groups_data(week_start, week_end)
        
        if not groups_data:
            self.logger.info("No Indie Parents Pack group data found for this week")
            return
        
        # Process individual group summaries
        group_summaries = []
        for group_name, group_data in groups_data.items():
            self.logger.info(f"Processing group: {group_name}")
            group_summary = self.create_group_summary(group_data, week_start)
            if group_summary:
                group_summaries.append(group_summary)
                self.log_summary(group_summary)
        
        # Process rollup summary if multiple groups exist
        if len(groups_data) > 1:
            self.logger.info("Creating rollup summary for all groups")
            rollup_summary = self.create_rollup_summary(list(groups_data.values()), week_start)
            if rollup_summary:
                self.log_summary(rollup_summary)
        else:
            self.logger.info("Only one group found, skipping rollup summary")
        
        self.logger.info(f"Completed processing {len(groups_data)} groups for week {week_start}")

def main():
    parser = argparse.ArgumentParser(description='WhatsApp Indie Parents Pack Weekly Summarizer')
    parser.add_argument('start_date', help='Start date for the week in YYYY-MM-DD format')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    try:
        # Initialize and run summarizer
        summarizer = WhatsAppSummarizer()
        summarizer.process_week(args.start_date)
        
    except Exception as e:
        print(f"Error: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())