<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0" 
                xmlns:html="http://www.w3.org/TR/REC-html40"
                xmlns:image="http://www.google.com/schemas/sitemap-image/1.1"
                xmlns:sitemap="http://www.sitemaps.org/schemas/sitemap/0.9"
                xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
    <xsl:output method="html" version="1.0" encoding="UTF-8" indent="yes"/>
    <xsl:template match="/">
        <html xmlns="http://www.w3.org/1999/xhtml">
            <head>
                <title>XML Sitemap</title>
                <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
                <style type="text/css">
                    body {
                        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
                        color: #333;
                        max-width: 1200px;
                        margin: 0 auto;
                        padding: 20px;
                        background: #f9f9f9;
                    }
                    h1 { font-size: 24px; font-weight: 600; margin-bottom: 10px; }
                    p { font-size: 14px; color: #666; margin-bottom: 20px; }
                    .card {
                        background: #fff;
                        border: 1px solid #e1e4e8;
                        border-radius: 6px;
                        box-shadow: 0 1px 3px rgba(0,0,0,0.05);
                        overflow: hidden;
                    }
                    table { width: 100%; border-collapse: collapse; }
                    th {
                        text-align: left; padding: 12px 15px; background: #f6f8fa;
                        border-bottom: 1px solid #e1e4e8; font-size: 13px; font-weight: 600;
                        color: #24292e; text-transform: uppercase;
                    }
                    td { padding: 12px 15px; border-bottom: 1px solid #eaecef; font-size: 13px; color: #586069; }
                    tr:hover td { background: #f6f8fa; }
                    a { color: #0366d6; text-decoration: none; }
                    a:hover { text-decoration: underline; }
                    .badge {
                        display: inline-block; padding: 2px 6px; font-size: 11px; font-weight: 600;
                        color: #fff; background-color: #28a745; border-radius: 4px; margin-right: 5px;
                    }
                </style>
            </head>
            <body>
                <h1>XML Sitemap</h1>
                <p>
                    This is an XML Sitemap generated for search engines (like Google) to discover your content.<br/>
                    It contains <strong><xsl:value-of select="count(sitemap:sitemapindex/sitemap:sitemap) + count(sitemap:urlset/sitemap:url)"/></strong> entries.
                </p>
                
                <div class="card">
                    <!-- SITEMAP INDEX -->
                    <xsl:if test="count(sitemap:sitemapindex/sitemap:sitemap) &gt; 0">
                        <table>
                            <thead>
                                <tr>
                                    <th>Sitemap URL</th>
                                    <th>Last Modified</th>
                                </tr>
                            </thead>
                            <tbody>
                                <xsl:for-each select="sitemap:sitemapindex/sitemap:sitemap">
                                    <tr>
                                        <td><a href="{sitemap:loc}"><xsl:value-of select="sitemap:loc"/></a></td>
                                        <td><xsl:value-of select="sitemap:lastmod"/></td>
                                    </tr>
                                </xsl:for-each>
                            </tbody>
                        </table>
                    </xsl:if>

                    <!-- URL SET -->
                    <xsl:if test="count(sitemap:urlset/sitemap:url) &gt; 0">
                        <table>
                            <thead>
                                <tr>
                                    <th>Page URL</th>
                                    <th>Images</th>
                                    <th>Last Modified</th>
                                </tr>
                            </thead>
                            <tbody>
                                <xsl:for-each select="sitemap:urlset/sitemap:url">
                                    <tr>
                                        <td><a href="{sitemap:loc}"><xsl:value-of select="sitemap:loc"/></a></td>
                                        <td>
                                            <xsl:if test="count(image:image) &gt; 0">
                                                <span class="badge"><xsl:value-of select="count(image:image)"/> Images</span>
                                                <div style="margin-top: 4px;">
                                                    <xsl:for-each select="image:image">
                                                        <a href="{image:loc}" target="_blank" style="color:#6a737d; margin-right:10px;">
                                                            📷 <xsl:value-of select="image:title"/>
                                                        </a>
                                                    </xsl:for-each>
                                                </div>
                                            </xsl:if>
                                        </td>
                                        <td><xsl:value-of select="sitemap:lastmod"/></td>
                                    </tr>
                                </xsl:for-each>
                            </tbody>
                        </table>
                    </xsl:if>
                </div>
            </body>
        </html>
    </xsl:template>
</xsl:stylesheet>