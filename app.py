#!/usr/bin/env python3
"""
WhatsApp Summarizer with Bubble.io Integration
Generates weekly summaries and sends them to Bubble endpoint
"""

import mysql.connector
import os
import json
import re
import argparse
import logging
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
from collections import defaultdict, Counter
from dataclasses import dataclass, asdict
from openai import OpenAI
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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
    week_end: str
    message_count: int
    participants: int
    links: int
    summary: str
    keywords: List[str]

@dataclass
class BubblePayload:
    summary_text: str
    week_start: str
    week_end: str
    total_messages: int
    total_groups: int
    total_participants: int
    total_links: int
    generation_timestamp: str
    status: str
    error_message: str = ""  # Default to empty string instead of None
    
    def to_dict(self):
        """Convert to dictionary, always include error_message field"""
        return asdict(self)

class WhatsAppSummarizer:
    def __init__(self):
        self.setup_logging()
        self.setup_db_config()
        self.setup_openai_client()
        self.setup_bubble_config()
        self.setup_http_session()
        self.thepack_articles = self.load_thepack_articles()
        self.system_context = self.build_system_context()
        
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
        
        required_fields = ['database', 'user', 'password']
        missing_fields = [field for field in required_fields if not self.db_config.get(field)]
        
        if missing_fields:
            raise ValueError(f"Missing required environment variables: {missing_fields}")
    
    def setup_openai_client(self):
        """Setup OpenAI client"""
        openai_api_key = os.getenv('OPENAI_API_KEY')
        if not openai_api_key:
            raise ValueError("OPENAI_API_KEY environment variable is required")
        
        self.openai_client = OpenAI(api_key=openai_api_key)
        self.logger.info("OpenAI client initialized")
    
    def setup_bubble_config(self):
        """Setup Bubble API configuration"""
        self.bubble_endpoint = os.getenv('BUBBLE_ENDPOINT_URL')
        self.bubble_api_key = os.getenv('BUBBLE_API_KEY')  # Optional: if Bubble requires API key
        
        if not self.bubble_endpoint:
            self.logger.warning("⚠️ BUBBLE_ENDPOINT_URL not set - summaries will only be saved locally")
        else:
            self.logger.info(f"Bubble endpoint configured: {self.bubble_endpoint}")
    
    def setup_http_session(self):
        """Setup requests session with retry logic"""
        self.session = requests.Session()
        
        retry_strategy = Retry(
            total=3,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["POST"],
            backoff_factor=2
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # Set default headers
        self.session.headers.update({
            'Content-Type': 'application/json',
            'User-Agent': 'ThePack-Summarizer/1.0'
        })
        
        # Add API key if provided
        if self.bubble_api_key:
            self.session.headers.update({
                'Authorization': f'Bearer {self.bubble_api_key}'
            })
    
    def load_thepack_articles(self) -> List[Dict]:
        """Load thePack.in article catalog from file"""
        articles_file = os.getenv('THEPACK_ARTICLES_FILE', 'thepack_articles.json')
        
        self.logger.info(f"Looking for articles file: {articles_file}")
        
        if os.path.exists(articles_file):
            try:
                with open(articles_file, 'r', encoding='utf-8') as f:
                    articles = json.load(f)
                    self.logger.info(f"✅ Loaded {len(articles)} thePack.in articles from {articles_file}")
                    
                    if articles:
                        titles = [a.get('title', 'Untitled')[:50] for a in articles[:3]]
                        self.logger.info(f"Sample articles: {', '.join(titles)}")
                    
                    return articles
            except json.JSONDecodeError as e:
                self.logger.error(f"JSON parsing error in articles file: {e}")
            except Exception as e:
                self.logger.error(f"Could not load articles file: {e}")
        else:
            self.logger.warning(f"❌ Articles file not found at: {os.path.abspath(articles_file)}")
            self.logger.warning("Create 'thepack_articles.json' in the same directory as app.py")
        
        return []
    
    def build_system_context(self) -> str:
        """Build system context that will be stored in GPT's 'memory' (system message)"""
        
        articles_ref = ""
        if self.thepack_articles:
            articles_ref = "\n\n**CRITICAL: thePack.in Article Catalog - YOU MUST USE THESE LINKS:**\n"
            articles_ref += "=" * 80 + "\n"
            articles_ref += "Match issues to articles by searching for RELEVANT KEYWORDS in tags or titles.\n"
            articles_ref += "For example:\n"
            articles_ref += "- Diwali anxiety → Look for tags: anxiety, diwali, stress, festivals\n"
            articles_ref += "- Skin issues → Look for tags: skin, allergies, itching, scratching\n"
            articles_ref += "- Training → Look for tags: training, behaviour, behavior\n"
            articles_ref += "- Biting → Look for tags: biting, puppy, aggression\n"
            articles_ref += "=" * 80 + "\n\n"
            
            # Group articles by common themes
            articles_by_theme = {}
            for article in self.thepack_articles:
                title = article.get('title', 'Untitled')
                url = article.get('url', '')
                tags = article.get('tags', [])
                
                searchable = f"{title.lower()} {' '.join(str(t) for t in tags)}"
                
                themes = []
                if any(word in searchable for word in ['anxiety', 'stress', 'diwali', 'festival', 'fear', 'scared', 'loud', 'noise']):
                    themes.append('ANXIETY/STRESS')
                if any(word in searchable for word in ['skin', 'itch', 'scratch', 'allerg', 'rash', 'patch']):
                    themes.append('SKIN ISSUES')
                if any(word in searchable for word in ['train', 'behav', 'bite', 'aggress']):
                    themes.append('TRAINING/BEHAVIOR')
                if any(word in searchable for word in ['feed', 'food', 'nutrition', 'diet', 'eat']):
                    themes.append('NUTRITION')
                if any(word in searchable for word in ['health', 'vet', 'disease', 'illness', 'sick']):
                    themes.append('HEALTH')
                
                if not themes:
                    themes.append('GENERAL')
                
                for theme in themes:
                    if theme not in articles_by_theme:
                        articles_by_theme[theme] = []
                    articles_by_theme[theme].append({
                        'title': title,
                        'url': url,
                        'tags': tags[:10]
                    })
            
            for theme, theme_articles in sorted(articles_by_theme.items()):
                articles_ref += f"\n**{theme} ARTICLES:**\n"
                for article in theme_articles[:5]:
                    tags_str = ", ".join([str(t) for t in article['tags']])
                    articles_ref += f"• {article['title']}\n"
                    articles_ref += f"  Tags: {tags_str}\n"
                    articles_ref += f"  URL: {article['url']}\n\n"
            
            articles_ref += "\n**MATCHING INSTRUCTIONS:**\n"
            articles_ref += "1. Read the issue/problem discussed in messages\n"
            articles_ref += "2. Identify KEY KEYWORDS (anxiety, skin, training, etc.)\n"
            articles_ref += "3. Look in the RELEVANT THEME SECTION above\n"
            articles_ref += "4. Find articles whose TAGS match those keywords\n"
            articles_ref += "5. ALWAYS prefer linking to articles over 'Guide coming soon'\n"
            articles_ref += "6. If you find a matching article, USE IT!\n\n"
            
        else:
            articles_ref = "\n\n**No article catalog available.** Use 'Guide coming soon' for all article links.\n"
        
        system_context = f"""You are thePack.in's content editor creating weekly beginner-friendly summaries of WhatsApp community conversations.

**Your Role:**
Create clear, actionable pet care content for first-time dog parents in India (70% women, age 25-40, Tier-1 cities, college/post-grad education).

**Audience Needs:**
- Read on mobile devices
- Prefer short, simple sentences
- Need step-by-step guidance they can use immediately
- Want to avoid jargon or need explanations for technical terms

**Output Structure - Always follow this exact format:**

# thePack.in Pet Parent's Community | Weekly Summary: [DATE_RANGE]

## This week in 60 seconds
[120-150 words: Friendly, witty overview of the week's main themes and discussions]

## Top 3 issues faced by pet parents in India

### 1. [Issue Title]
**What parents asked:** [2-line summary of the problem]

**What experts recommend:**
1. [First actionable tip in simple language]
2. [Second actionable tip]
3. [Third actionable tip]

**Learn more:** [Article Title] 👉🏽 [URL] (or "Guide coming soon")

[Repeat for issues 2 and 3]

## New things that Pet Parents Tried this Week

**[New Idea #1]:** [2-line description of what it is and how it helps dogs]
**Try it yourself:** [Article link if available, or external link shared by parents, or skip if none]
[Add short safety caveat if needed]

[Repeat for up to 5 new ideas]

## What pet parents are reading at thePack
1. **[Article/Event Title]:** [Why it's noteworthy - "first time discussed," "spike in volume," "new resource"]
[Repeat for up to 4 items]

## Safety Note
[Single sentence about urgent signs requiring vet visit: blood in stool, high fever, extreme lethargy, etc.]

---
*Do you have questions about your dog? Reply in your WhatsApp group or email me on shobhit@thepack.in*

**Writing Rules:**
- Use short paragraphs (2-3 sentences max)
- Write complete sentences, not bullet fragments
- Explain technical terms in one short clause
- Keep total summary under 1,500 words
- Never recommend prescription medicine doses
- Never invent URLs - use "Guide coming soon" if no match

**Article Linking:**
- Match problems to articles by title and tags
- Prefer beginner-friendly, evergreen content
- Format: "Article Title 👉🏽 URL"
- If no good match exists, write "Guide coming soon"
{articles_ref}

**Important:** You will receive only the message data and statistics for each week. Use this system context as your reference guide for format, tone, and article matching."""

        return system_context
    
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
        emoji_pattern = re.compile(
            "["
            "\U0001F1E0-\U0001F1FF"
            "\U0001F300-\U0001F5FF"
            "\U0001F600-\U0001F64F"
            "\U0001F680-\U0001F6FF"
            "\U0001F700-\U0001F77F"
            "\U0001F780-\U0001F7FF"
            "\U0001F800-\U0001F8FF"
            "\U0001F900-\U0001F9FF"
            "\U0001FA00-\U0001FA6F"
            "\U0001FA70-\U0001FAFF"
            "\U00002702-\U000027B0"
            "\U000024C2-\U0001F251" 
            "]+"
        )
        return emoji_pattern.sub('', text).strip()
    
    def get_week_start_end(self, start_date_str: str) -> Tuple[str, str, str]:
        """Convert start date to week start (Monday) and end (Sunday) in IST"""
        try:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
            
            days_since_monday = start_date.weekday()
            monday = start_date - timedelta(days=days_since_monday)
            sunday = monday + timedelta(days=6)
            
            week_start = monday.strftime('%Y-%m-%d')
            week_end_date = sunday.strftime('%Y-%m-%d')
            week_end_query = (sunday + timedelta(days=1)).strftime('%Y-%m-%d')
            
            self.logger.info(f"Processing week: {week_start} to {week_end_date}")
            return week_start, week_end_date, week_end_query
            
        except ValueError:
            raise ValueError("Invalid date format. Use YYYY-MM-DD")
    
    def get_indie_groups_data(self, week_start: str, week_end_query: str) -> Dict[str, GroupData]:
        """Fetch data for Indie Parents Pack groups for the specified week"""
        
        conn = self.get_db_connection()
        cursor = conn.cursor()
        
        try:
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
            cursor.execute(debug_query, (week_start, week_end_query, '%Indie Parents Pack%'))
            debug_results = cursor.fetchall()
            
            if not debug_results:
                self.logger.warning("❌ No groups found")
                return {}
            
            for group_name, raw_count in debug_results:
                self.logger.info(f"  📱 {group_name}: {raw_count} raw messages")
            
            main_query = """
            SELECT GroupName, Message, Links
            FROM WhatsAppExport 
            WHERE CreatedDate >= %s AND CreatedDate < %s
            AND GroupName LIKE %s
            AND Message IS NOT NULL AND Message != ''
            ORDER BY GroupName, CreatedDate
            """
            
            cursor.execute(main_query, (week_start, week_end_query, '%Indie Parents Pack%'))
            rows = cursor.fetchall()
            
            groups_data = defaultdict(lambda: GroupData(
                group_name='',
                message_count=0,
                participants=set(),
                links=[],
                messages=[]
            ))
            
            for row in rows:
                group_name, message, links = row
                
                clean_message = self.remove_emojis(message)
                if not clean_message.strip():
                    continue
                
                if group_name not in groups_data:
                    groups_data[group_name].group_name = group_name
                
                groups_data[group_name].message_count += 1
                groups_data[group_name].messages.append(clean_message)
                
                if links:
                    groups_data[group_name].links.append(links)
            
            self.logger.info(f"Groups with usable content: {len(groups_data)}")
            return dict(groups_data)
            
        finally:
            cursor.close()
            conn.close()
    
    def prepare_weekly_message_data(self, groups_data: Dict[str, GroupData], week_start: str, week_end: str) -> List[str]:
        """Prepare ALL weekly messages in batches to avoid token limits"""
        
        total_messages = sum(g.message_count for g in groups_data.values())
        
        all_words = []
        for group_data in groups_data.values():
            for msg in group_data.messages:
                words = re.findall(r'\b\w{4,}\b', msg.lower())
                all_words.extend(words)
        
        word_freq = Counter(all_words)
        stop_words = {'that', 'this', 'with', 'have', 'from', 'they', 'will', 'been', 'what', 'when', 'your', 'also', 'there', 'would', 'about', 'their', 'which', 'were', 'here', 'just', 'some', 'know', 'make', 'like', 'then', 'than', 'them', 'these', 'those', 'could', 'should', 'does', 'https', 'www', 'com', 'want', 'need', 'group', 'pack', 'indie', 'parent', 'parents'}
        top_topics = [(word, count) for word, count in word_freq.most_common(50) if word not in stop_words][:15]
        
        all_messages = []
        for group_name, group_data in groups_data.items():
            group_short = group_name.split('🐶')[-1].strip() or group_name
            
            for msg in group_data.messages:
                all_messages.append(f"[{group_short}] {msg}")
        
        self.logger.info(f"Total messages to process: {len(all_messages)}")
        
        batch_size = 150
        batches = []
        
        for i in range(0, len(all_messages), batch_size):
            batch_messages = all_messages[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (len(all_messages) + batch_size - 1) // batch_size
            
            batch_prompt = f"""**Week:** {week_start} to {week_end}
**Groups:** {len(groups_data)}
**Total Messages:** {total_messages}
**Top Topics:** {', '.join([f'{word}({count})' for word, count in top_topics[:12]])}

**Batch {batch_num} of {total_batches} ({len(batch_messages)} messages):**
{chr(10).join(batch_messages)}

{"Analyze these messages and remember the key themes, issues, and discussions. I will send you more batches." if batch_num < total_batches else "This is the final batch. Now generate the complete weekly summary following your system instructions, incorporating insights from all batches."}"""
            
            batches.append(batch_prompt)
        
        self.logger.info(f"Split into {len(batches)} batches of ~{batch_size} messages each")
        return batches
    
    def generate_editorial_summary(self, groups_data: Dict[str, GroupData], week_start: str, week_end: str) -> str:
        """Generate editorial summary using OpenAI with batched messages"""
        
        message_batches = self.prepare_weekly_message_data(groups_data, week_start, week_end)
        
        self.logger.info(f"Processing {len(message_batches)} batches with OpenAI...")
        
        try:
            batch_summaries = []
            total_tokens_used = 0
            
            for i, batch_prompt in enumerate(message_batches, 1):
                self.logger.info(f"Sending batch {i}/{len(message_batches)} to OpenAI...")
                
                if i < len(message_batches):
                    instruction = f"""{batch_prompt}

Summarize the key themes, issues, and topics from this batch in 3-4 bullet points. Focus on:
- Main problems pet parents discussed
- New ideas or solutions mentioned
- Trending topics or concerns"""
                    
                    response = self.openai_client.chat.completions.create(
                        model="gpt-3.5-turbo-16k",
                        messages=[
                            {"role": "system", "content": "You extract key themes from WhatsApp pet parent conversations. Be concise."},
                            {"role": "user", "content": instruction}
                        ],
                        temperature=0.5,
                        max_tokens=300
                    )
                    
                    batch_summary = response.choices[0].message.content.strip()
                    batch_summaries.append(f"Batch {i} themes:\n{batch_summary}")
                    
                    usage = response.usage
                    total_tokens_used += usage.total_tokens
                    self.logger.info(f"Batch {i} - Tokens: {usage.total_tokens}")
                    
                else:
                    self.logger.info("Processing final batch and generating complete summary...")
                    
                    previous_themes = "\n\n".join(batch_summaries)
                    
                    if self.thepack_articles:
                        self.logger.info(f"Using {len(self.thepack_articles)} articles for linking")
                    else:
                        self.logger.warning("No articles available - all links will show 'Guide coming soon'")
                    
                    final_instruction = f"""{batch_prompt}

**Previous batches summary:**
{previous_themes}

**CRITICAL ARTICLE MATCHING INSTRUCTIONS:**
You have a comprehensive article catalog in your system instructions organized by themes:
- ANXIETY/STRESS articles (for Diwali stress, anxiety, fear, loud noises)
- SKIN ISSUES articles (for itching, scratching, skin patches, allergies)
- TRAINING/BEHAVIOR articles (for biting, aggression, training needs)
- NUTRITION articles (for feeding, food, diet)
- HEALTH articles (for medical concerns)

For EACH issue in "Top 3 issues":
1. Identify the theme (anxiety/skin/training/health)
2. Look in that theme section of your article catalog
3. Find articles with matching tags
4. USE THE ACTUAL ARTICLE LINK - don't write "Guide coming soon" if a match exists!

Example matching:
- "Diwali stress" → Look in ANXIETY/STRESS → Use article about anxiety/diwali/festivals
- "Skin patches" → Look in SKIN ISSUES → Use article about skin/allergies/itching
- "Puppy biting" → Look in TRAINING/BEHAVIOR → Use article about biting/training

Now generate the complete weekly editorial summary with PROPER ARTICLE LINKS from the catalog."""
                    
                    response = self.openai_client.chat.completions.create(
                        model="gpt-3.5-turbo-16k",
                        messages=[
                            {"role": "system", "content": self.system_context},
                            {"role": "user", "content": final_instruction}
                        ],
                        temperature=0.7,
                        max_tokens=2500
                    )
                    
                    editorial_summary = response.choices[0].message.content.strip()
                    
                    usage = response.usage
                    total_tokens_used += usage.total_tokens
                    self.logger.info(f"Final batch - Tokens: {usage.total_tokens}")
            
            self.logger.info("✅ Editorial summary generated successfully")
            self.logger.info(f"Total tokens used across all batches: {total_tokens_used}")
            
            # Validate summary was generated
            if not editorial_summary or len(editorial_summary) < 100:
                raise ValueError("Generated summary is too short or empty")
            
            return editorial_summary
            
        except Exception as e:
            self.logger.error(f"Error generating editorial summary: {e}")
            raise
    
    def _generate_fallback_summary(self, groups_data: Dict[str, GroupData], week_start: str, week_end: str) -> str:
        """Generate basic fallback summary if AI fails"""
        total_messages = sum(g.message_count for g in groups_data.values())
        
        return f"""# thePack.in Pet Parent's Community | Weekly Summary: {week_start} to {week_end}

## This week in 60 seconds
This week, our community of {len(groups_data)} groups shared {total_messages} messages about pet care and support. Pet parents actively discussed their experiences and sought advice from the community.

## Top 3 issues faced by pet parents in India
Due to a technical error, detailed analysis is not available this week. Please check back next week.

## New things that Pet Parents Tried this Week
Analysis pending due to technical issues.

## What pet parents are reading at thePack
Analysis pending due to technical issues.

## Safety Note
If your dog shows urgent signs like blood in stool, high fever, or extreme lethargy, visit your vet immediately.

---
*Do you have questions about your dog? Reply in your WhatsApp group or email me on shobhit@thepack.in*
"""
    
    def send_to_bubble(self, payload: BubblePayload) -> bool:
        """Send summary data to Bubble endpoint"""
        
        if not self.bubble_endpoint:
            self.logger.warning("Bubble endpoint not configured - skipping upload")
            return False
        
        try:
            self.logger.info(f"Sending data to Bubble: {self.bubble_endpoint}")
            
            response = self.session.post(
                self.bubble_endpoint,
                json=payload.to_dict(),
                timeout=30
            )
            
            response.raise_for_status()
            
            self.logger.info(f"✅ Successfully sent data to Bubble (Status: {response.status_code})")
            
            # Log response details
            try:
                response_data = response.json()
                self.logger.info(f"Bubble response: {response_data}")
            except:
                self.logger.info(f"Bubble response: {response.text[:200]}")
            
            return True
            
        except requests.exceptions.Timeout:
            self.logger.error("❌ Bubble API timeout after 30 seconds")
            return False
        except requests.exceptions.RequestException as e:
            self.logger.error(f"❌ Failed to send data to Bubble: {e}")
            if hasattr(e, 'response') and e.response is not None:
                self.logger.error(f"Response status: {e.response.status_code}")
                self.logger.error(f"Response body: {e.response.text[:500]}")
            return False
        except Exception as e:
            self.logger.error(f"❌ Unexpected error sending to Bubble: {e}")
            return False
    
    def save_summary_to_file(self, summary: str, week_start: str):
        """Save the editorial summary to a markdown file"""
        filename = f"thepack_summary_{week_start}.md"
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(summary)
            
            self.logger.info(f"✅ Editorial summary saved to: {filename}")
            return filename
        except Exception as e:
            self.logger.error(f"Failed to save summary file: {e}")
            return None
    
    def process_week(self, start_date: str):
        """Main function to process editorial summary for a specific week"""
        
        self.logger.info(f"Starting editorial summary generation for week starting {start_date}")
        
        error_message = None
        editorial_summary = None
        groups_data = {}
        week_start = week_end = ""
        
        try:
            week_start, week_end, week_end_query = self.get_week_start_end(start_date)
            
            groups_data = self.get_indie_groups_data(week_start, week_end_query)
            
            if not groups_data:
                error_message = "No data found for this week"
                self.logger.warning(error_message)
                editorial_summary = self._generate_fallback_summary({}, week_start, week_end)
            else:
                editorial_summary = self.generate_editorial_summary(groups_data, week_start, week_end)
            
            # Print to console
            print("\n" + "="*80)
            print(editorial_summary)
            print("="*80 + "\n")
            
            # Save to file
            self.save_summary_to_file(editorial_summary, week_start)
            
        except Exception as e:
            error_message = str(e)
            self.logger.error(f"Error during processing: {error_message}")
            
            # Try to generate fallback summary
            try:
                editorial_summary = self._generate_fallback_summary(groups_data, week_start, week_end)
            except:
                editorial_summary = f"# Error generating summary\n\nError: {error_message}"
        
        finally:
            # Always try to send data to Bubble, even if there were errors
            try:
                total_messages = sum(g.message_count for g in groups_data.values()) if groups_data else 0
                total_links = sum(len(g.links) for g in groups_data.values()) if groups_data else 0
                
                payload = BubblePayload(
                    summary_text=editorial_summary or "Failed to generate summary",
                    week_start=week_start or start_date,
                    week_end=week_end or start_date,
                    total_messages=total_messages,
                    total_groups=len(groups_data) if groups_data else 0,
                    total_participants=0,  # Not tracking participants
                    total_links=total_links,
                    generation_timestamp=datetime.now().isoformat(),
                    status="success" if not error_message else "error",
                    error_message=error_message if error_message else ""
                )
                
                bubble_success = self.send_to_bubble(payload)
                
                if bubble_success:
                    self.logger.info("✅ Data successfully sent to Bubble")
                else:
                    self.logger.warning("⚠️ Failed to send data to Bubble, but local file was saved")
                    
            except Exception as bubble_error:
                self.logger.error(f"❌ Critical error sending to Bubble: {bubble_error}")
        
        self.logger.info(f"Completed processing for week {week_start}")
        
        # Return success status
        return error_message is None

def main():
    parser = argparse.ArgumentParser(description='WhatsApp thePack.in Editorial Summarizer (OpenAI)')
    parser.add_argument('start_date', help='Start date for the week in YYYY-MM-DD format')
    parser.add_argument('--articles', help='Path to thePack.in articles JSON file', default=None)
    
    args = parser.parse_args()
    
    if args.articles:
        os.environ['THEPACK_ARTICLES_FILE'] = args.articles
    
    try:
        summarizer = WhatsAppSummarizer()
        success = summarizer.process_week(args.start_date)
        
        if success:
            print("\n✅ Summary generation completed successfully")
            return 0
        else:
            print("\n⚠️ Summary generation completed with warnings/errors")
            return 1
        
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        logging.exception("Fatal error occurred")
        return 1

if __name__ == "__main__":
    exit(main())

# AWS Lambda Handler
def lambda_handler(event, context):
    """
    AWS Lambda handler function
    Automatically calculates the previous week (Monday to Sunday) and generates summary
    """
    import json
    from datetime import datetime, timedelta
    
    # Calculate last week's Monday
    today = datetime.now()
    days_since_monday = today.weekday()
    this_monday = today - timedelta(days=days_since_monday)
    last_monday = this_monday - timedelta(days=7)
    start_date = last_monday.strftime('%Y-%m-%d')
    
    print(f"Lambda triggered - Processing week starting: {start_date}")
    
    try:
        summarizer = WhatsAppSummarizer()
        success = summarizer.process_week(start_date)
        
        return {
            'statusCode': 200 if success else 500,
            'body': json.dumps({
                'message': 'Summary generated successfully' if success else 'Generation failed',
                'week_start': start_date,
                'success': success
            })
        }
    except Exception as e:
        print(f"Lambda error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'week_start': start_date
            })
        }