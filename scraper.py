import re
import json
import os
from collections import defaultdict
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin, urldefrag

# ─── File to persist stats across crawler restarts ───
STATS_FILE = "crawler_stats.json"

# ─── Stop words to ignore when counting words ───
STOPWORDS = {
    "a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are", "aren't",
    "as", "at", "be", "because", "been", "before", "being", "below", "between", "both", "but", "by",
    "can't", "cannot", "could", "couldn't", "did", "didn't", "do", "does", "doesn't", "doing", "don't",
    "down", "during", "each", "few", "for", "from", "further", "had", "hadn't", "has", "hasn't",
    "have", "haven't", "having", "he", "he'd", "he'll", "he's", "her", "here", "here's", "hers",
    "herself", "him", "himself", "his", "how", "how's", "i", "i'd", "i'll", "i'm", "i've", "if",
    "in", "into", "is", "isn't", "it", "it's", "its", "itself", "let's", "me", "more", "most",
    "mustn't", "my", "myself", "no", "nor", "not", "of", "off", "on", "once", "only", "or", "other",
    "ought", "our", "ours", "ourselves", "out", "over", "own", "same", "shan't", "she", "she'd",
    "she'll", "she's", "should", "shouldn't", "so", "some", "such", "than", "that", "that's", "the",
    "their", "theirs", "them", "themselves", "then", "there", "there's", "these", "they", "they'd",
    "they'll", "they're", "they've", "this", "those", "through", "to", "too", "under", "until",
    "up", "very", "was", "wasn't", "we", "we'd", "we'll", "we're", "we've", "were", "weren't",
    "what", "what's", "when", "when's", "where", "where's", "which", "while", "who", "who's",
    "whom", "why", "why's", "with", "won't", "would", "wouldn't", "you", "you'd", "you'll",
    "you're", "you've", "your", "yours", "yourself", "yourselves"
}

# ─── Load stats from file (so we don't lose data if crawler restarts) ───
def load_stats():
    if os.path.exists(STATS_FILE):
        with open(STATS_FILE, "r") as f:
            data = json.load(f)
            return (
                set(data.get("unique_pages", [])),
                data.get("longest_page", {"url": "", "count": 0}),
                defaultdict(int, data.get("word_freq", {})),
                defaultdict(int, data.get("subdomains", {}))
            )
    return set(), {"url": "", "count": 0}, defaultdict(int), defaultdict(int)

# ─── Save stats to file ───
def save_stats(unique_pages, longest_page, word_freq, subdomains):
    with open(STATS_FILE, "w") as f:
        json.dump({
            "unique_pages": list(unique_pages),
            "longest_page": longest_page,
            "word_freq": dict(word_freq),
            "subdomains": dict(subdomains)
        }, f)

# ─── Load existing stats when the module starts ───
unique_pages, longest_page, word_freq, subdomains = load_stats()

# ─── Main scraper function (called once per page) ───
def scraper(url, resp):
    links = extract_next_links(url, resp)
    return [link for link in links if is_valid(link)]

def extract_next_links(url, resp):
    links = []

    # Skip failed responses
    if resp.status != 200:
        return links
    if resp.raw_response is None or resp.raw_response.content is None:
        return links

    # Skip very large files (over 5MB) — likely not useful text pages
    content_length = len(resp.raw_response.content)
    if content_length > 5 * 1024 * 1024:
        return links

    try:
        soup = BeautifulSoup(resp.raw_response.content, "html.parser")

        # ─── Extract visible text and count words ───
        text = soup.get_text(separator=" ")
        words = re.findall(r"[a-zA-Z]{2,}", text.lower())
        word_count = len(words)

        # Skip low-information pages (less than 50 words)
        if word_count < 50:
            return links

        # ─── Track unique pages (strip fragment just in case) ───
        defrag_url, _ = urldefrag(url)
        unique_pages.add(defrag_url)

        # ─── Track longest page ───
        if word_count > longest_page["count"]:
            longest_page["url"] = defrag_url
            longest_page["count"] = word_count

        # ─── Count word frequencies (ignore stop words) ───
        for word in words:
            if word not in STOPWORDS:
                word_freq[word] += 1

        # ─── Track subdomains ───
        parsed = urlparse(defrag_url)
        hostname = parsed.hostname
        if hostname and hostname.endswith(".uci.edu"):
            subdomains[hostname] += 1

        # ─── Save stats to file after every page ───
        save_stats(unique_pages, longest_page, word_freq, subdomains)

        # ─── Extract all links from the page ───
        for anchor in soup.find_all("a", href=True):
            href = anchor["href"].strip()
            if not href:
                continue
            absolute_url = urljoin(resp.url, href)
            defragmented_url, _ = urldefrag(absolute_url)
            links.append(defragmented_url)

    except Exception as e:
        print(f"Error parsing {url}: {e}")

    return links

def is_valid(url):
    try:
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"}:
            return False

        # Only allow the four required UCI domains
        allowed_domains = (
            ".ics.uci.edu",
            ".cs.uci.edu",
            ".informatics.uci.edu",
            ".stat.uci.edu",
        )
        hostname = parsed.hostname
        if hostname is None:
            return False
        if not any(hostname == d[1:] or hostname.endswith(d) for d in allowed_domains):
            return False

        # Detect repeating directories (trap)
        if re.match(r".*?(/[^/]+)/+?\1/+?\1.*", parsed.path):
            return False

        # Detect very long paths (trap)
        if len(parsed.path.split("/")) > 20:
            return False

        # Block non-text file types
        return not re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())

    except TypeError:
        print("TypeError for ", parsed)
        raise

# Call this after crawling is done to print your report answers
def generate_report():
    print(f"\n{'='*50}")
    print(f"CRAWL REPORT")
    print(f"{'='*50}")

    print(f"\n1. Unique pages found: {len(unique_pages)}")

    print(f"\n2. Longest page:")
    print(f"   URL: {longest_page['url']}")
    print(f"   Word count: {longest_page['count']}")

    print(f"\n3. Top 50 most common words (excluding stop words):")
    sorted_words = sorted(word_freq.items(), key=lambda x: x[1], reverse=True)[:50]
    for i, (word, count) in enumerate(sorted_words, 1):
        print(f"   {i:2}. {word}: {count}")

    print(f"\n4. Subdomains found (alphabetical):")
    for subdomain in sorted(subdomains.keys()):
        print(f"   {subdomain}, {subdomains[subdomain]}")
