# -*- coding: utf-8 -*-

import requests
import json
import re

def get_fast_ratings(movie):
        season = 1
        lastSeason = 0
        while True:
                url = ('https://www.imdb.com/title/%s/episodes/?season=%d' % (movie.imdb, season))
                try:
                        r = requests.get(url, timeout=10)
                        r.raise_for_status()
                except requests.exceptions.HTTPError as errh:
                        movie.updateStatus = ('Http Error: %s' % errh)
                        return None
                except requests.exceptions.ConnectionError as errc:
                        movie.updateStatus = ('Connection Error: %s' % errc)
                        return None
                except requests.exceptions.Timeout as errt:
                        movie.updateStatus = ('Timeout: %s' % errt)
                        return None
                except requests.exceptions.RequestException as err:
                        movie.updateStatus = ('Error: %s' % err)
                        return None
                
                #seasons
                if lastSeason == 0:
                        start = r.text.find('<select id="bySeason" tconst="' + movie.imdb + '"')
                        if start > -1:
                                end = r.text.find('</select>', start)
				a = re.findall("<option \S*? value=\"(\d+)\">", r.text[start:end], re.DOTALL)
                                lastSeason = int(a[-1])
			else:
				movie.updateStatus = ('%s seasons not found in html' % movie.imdb)
				return None

                #ratings
                start = 0
                next = 0
                while next > -1:
                        next = r.text[start:].find('<div class="info" itemprop="episodes" itemscope itemtype="http://schema.org/TVEpisode">', 1)
                        if next > -1:
                                next += start
                        if start > 0:
                                a = re.findall("<meta itemprop=\"episodeNumber\" content=\"(\d+)\"/>", r.text[start:next])
                                if a:
                                        episodeNo = ('%dx%d' % (season, int(a[0])))
                                        b = re.findall("<strong><a href=\"/title/(tt\d+)/\"\n", r.text[start:next])
                                        if b:
                                                episodeIMDb = b[0]
                                        b = re.findall("<span class=\"ipl-rating-star__rating\">(\d+.?\d?)</span>\n", r.text[start:next])
                                        if b:
                                                movie.episodesRating[episodeNo] = ('%.1f' % float(b[0]))
                                        b = re.findall("<span class=\"ipl-rating-star__total-votes\">\((.*?)\)</span>\n", r.text[start:next])
                                        if b:
                                                movie.episodesVotes[episodeNo] = int(b[0].replace(",",""))
                        start = next
                        
                season += 1
                if season > lastSeason:
                        break
                        
        return True

def get_imdb(movie):
        try:
                r = requests.get('https://m.imdb.com/title/%s/' % movie.imdb, timeout=10)
                r.raise_for_status()
        except requests.exceptions.HTTPError as errh:
                movie.updateStatus = ('Http Error: %s' % errh)
                return None
        except requests.exceptions.ConnectionError as errc:
                movie.updateStatus = ('Connection Error: %s' % errc)
                return None
        except requests.exceptions.Timeout as errt:
                movie.updateStatus = ('Timeout: %s' % errt)
                return None
        except requests.exceptions.RequestException as err:
                movie.updateStatus = ('Error: %s' % err)
                return None

        pos = r.text.find('\"aggregateRating\":')
        if pos == -1:
                movie.updateStatus = ('IMDB ratings not found in html')
                return None
        a = r.text[pos+19:]
        b = a[:a.find('}')+1]
        imdb = {}
        imdb = json.loads(b)
        movie.ratingNew = imdb['ratingValue']
        movie.votesNew = imdb['ratingCount']
        
        if movie.type == 'movie':
                c = re.findall("Top Rated Movies #(\d+)", a)
                if c:
                        movie.top250New = int(c[0])

        return True
