import os
import threading
from datetime import datetime

class SitemapManager:
    def __init__(self, storage_path, base_url, max_urls=5000):
        self.storage_path = storage_path
        self.base_url = base_url.rstrip('/')
        self.max_urls = max_urls
        self.lock = threading.Lock()
        
        if not os.path.exists(self.storage_path):
            os.makedirs(self.storage_path)

    def _header(self):
        return '<?xml version="1.0" encoding="UTF-8"?>\n<?xml-stylesheet type="text/xsl" href="/static/sitemap.xsl"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9" xmlns:image="http://www.google.com/schemas/sitemap-image/1.1">\n'

    def _footer(self):
        return '</urlset>'

    def _entry(self, loc, lastmod, image_loc, title):
        # Escape XML entities
        def esc(s):
            if not s: return ""
            return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;").replace("'", "&apos;")
        
        s = f'  <url>\n    <loc>{esc(loc)}</loc>\n'
        if lastmod:
            s += f'    <lastmod>{lastmod}</lastmod>\n'
        s += f'    <image:image>\n      <image:loc>{esc(image_loc)}</image:loc>\n'
        if title:
            s += f'      <image:title>{esc(title)}</image:title>\n'
        s += '    </image:image>\n  </url>\n'
        return s

    def _get_sitemap_files(self):
        """Returns sorted list of sitemap files (e.g., sitemap_1.xml, sitemap_2.xml)."""
        files = [f for f in os.listdir(self.storage_path) if f.startswith("sitemap_") and f.endswith(".xml") and "index" not in f]
        # Sort by the number in the filename
        return sorted(files, key=lambda x: int(x.split('_')[1].split('.')[0]))

    def _update_index(self):
        """Regenerates the sitemap_index.xml file."""
        files = self._get_sitemap_files()
        content = '<?xml version="1.0" encoding="UTF-8"?>\n<?xml-stylesheet type="text/xsl" href="/static/sitemap.xsl"?>\n<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
        
        for f in files:
            path = os.path.join(self.storage_path, f)
            mtime = os.path.getmtime(path)
            lastmod = datetime.fromtimestamp(mtime).strftime('%Y-%m-%d')
            content += f'  <sitemap>\n    <loc>{self.base_url}/sitemaps/{f}</loc>\n    <lastmod>{lastmod}</lastmod>\n  </sitemap>\n'
        
        content += '</sitemapindex>'
        
        with open(os.path.join(self.storage_path, "sitemap_index.xml"), "w", encoding="utf-8") as f:
            f.write(content)

    def add_asset(self, page_url, lastmod, image_url, title):
        """Adds a single asset to the active sitemap file."""
        with self.lock:
            files = self._get_sitemap_files()
            current_file = None
            create_new = False
            index = 1
            
            if not files:
                create_new = True
            else:
                last_file = files[-1]
                index = int(last_file.split('_')[1].split('.')[0])
                
                # Check if file is full (approximate check by counting <url> tags)
                try:
                    with open(os.path.join(self.storage_path, last_file), 'r', encoding='utf-8') as f:
                        content = f.read()
                        if content.count('<url>') >= self.max_urls:
                            create_new = True
                            index += 1
                        else:
                            current_file = last_file
                except Exception:
                    create_new = True # Fallback if read fails

            if create_new:
                current_file = f"sitemap_{index}.xml"
                with open(os.path.join(self.storage_path, current_file), "w", encoding="utf-8") as f:
                    f.write(self._header() + self._footer())

            # Append to file: Remove footer, add entry, add footer back
            fpath = os.path.join(self.storage_path, current_file)
            try:
                with open(fpath, "r+", encoding="utf-8") as f:
                    content = f.read()
                    pos = content.rfind('</urlset>')
                    if pos != -1:
                        f.seek(pos)
                        f.write(self._entry(page_url, lastmod, image_url, title) + self._footer())
                        f.truncate()
            except Exception as e:
                print(f"Error writing to sitemap: {e}")
            
            self._update_index()

    def rebuild(self, assets_iterator):
        """
        Rebuilds all sitemaps from scratch using the provided iterator.
        assets_iterator: yields dict(page_url, lastmod, image_url, title)
        """
        with self.lock:
            # Delete existing sitemaps
            for f in os.listdir(self.storage_path):
                if f.startswith("sitemap_") and f.endswith(".xml"):
                    os.remove(os.path.join(self.storage_path, f))
            
            current_index = 1
            current_count = 0
            buffer = []
            
            def flush_buffer(idx):
                fname = f"sitemap_{idx}.xml"
                with open(os.path.join(self.storage_path, fname), "w", encoding="utf-8") as f:
                    f.write(self._header())
                    f.write("".join(buffer))
                    f.write(self._footer())
            
            for asset in assets_iterator:
                entry = self._entry(asset['page_url'], asset['lastmod'], asset['image_url'], asset['title'])
                buffer.append(entry)
                current_count += 1
                
                if current_count >= self.max_urls:
                    flush_buffer(current_index)
                    current_index += 1
                    current_count = 0
                    buffer = []
            
            if buffer or current_index == 1: # Ensure at least one file exists
                flush_buffer(current_index)
                
            self._update_index()
            return current_index # Return number of files created
