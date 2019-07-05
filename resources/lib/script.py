# -*- coding: utf-8 -*-

from resources.lib import kodiutils
from resources.lib import smartimdb
import xbmc
import xbmcgui
import xbmcaddon
import xbmcvfs
import json
import os
import sys
import unicodedata
import threading
import Queue
import time


ADDON 		= xbmcaddon.Addon()
ADDON_ID	= ADDON.getAddonInfo('id')
ADDON_NAME	= ADDON.getAddonInfo('name')
ADDON_PATH_DATA = xbmc.translatePath(os.path.join('special://profile/addon_data/', ADDON_ID)).replace('\\', '/') + '/'
ADDON_FAILED    = ADDON_PATH_DATA + 'failed/'
ADDON_QUEUED    = ADDON_PATH_DATA + 'queued/'
ADDON_TOUPDATE  = ADDON_PATH_DATA + 'to_update/'

#number of threads
ThreadsNumber   = 8
#update records for votes only
votesWithoutRating      = False
#entities to check
toCheck = ['movies', 'tvshows']
#toCheck = ['tvshows']


progress = xbmcgui.DialogProgressBG()

exitThreads = False
processingDB = True
processingEpisodes = True
countAll = 0
countQueued = 0
threadsLock = threading.Lock()

class Logger:
        def __init__(self):
                self.threadName = None
                
        def log(self, txt):
                if isinstance(txt, str):
                        txt = txt.decode('utf-8')
                if self.threadName:
                        message = u'%s[%s]: %s' % (ADDON_ID, self.threadName, txt)
                else:
                        message = u'%s: %s' % (ADDON_ID, txt)
                xbmc.log(msg=message.encode('utf-8'), level=xbmc.LOGDEBUG)

class TimeNotification:
        def __init__(self):
                self.sleep = 8000
                self.startTime = 0
                self.stopTime = 0
                self.items = 0

        def start(self):
                self.startTime = int(round(time.time()))

        def stop(self):
                self.stopTime = int(round(time.time()))
                if self.stopTime == self.startTime:
                        self.stopTime += 1
        
        def show(self, header, txt):
                if isinstance(header, str):
                        header = header.decode('utf-8')
                if isinstance(txt, str):
                        txt = txt.decode('utf-8')
                kodiutils.notification(header.encode('utf-8'), txt.encode('utf-8'), self.sleep)

        def showTime(self, header, txt):
                if isinstance(header, str):
                        header = header.decode('utf-8')
                if isinstance(txt, str):
                        txt = txt.decode('utf-8')

                min = (self.stopTime - self.startTime)/60
                sec = (self.stopTime - self.startTime)%60
                time = '%d s' % sec
                if min > 0:
                        time = '%d min %s' % (min, time)
                txt = txt.replace('{time}', time)
                        
                if self.items > 0:
                        speed = ('%.2f rating/s' % round(float(self.items)/(self.stopTime - self.startTime), 2))
                        txt = txt.replace('{speed}', speed)
                self.show(header, txt)

#parsing JSON VideoLibrary.GetMovies
class JSONMovieParser:
        def __init__(self, getMovies):
                if not hasattr(self, 'type'):
                        self.type = 'movie'
                self.id = getMovies[self.type + 'id']
                if 'title' in getMovies:
                        self.title = getMovies['title']
                if 'top250' in getMovies:
                        self.top250 = getMovies['top250']
                        self.top250New = -1
		self.update_imdbid = False
                if 'imdb' in getMovies['uniqueid']:
			self.imdb = getMovies['uniqueid']['imdb']
		elif 'unknown' in getMovies['uniqueid'] and getMovies['uniqueid']['unknown'].startswith('tt'):
			self.imdb = getMovies['uniqueid']['unknown']
		elif xbmcvfs.exists(ADDON_TOUPDATE + self.type + '_' + str(self.id) + '.dump'):
                        f = xbmcvfs.File(ADDON_TOUPDATE + self.type + '_' + str(self.id) + '.dump', 'r')
                        try:
                                toUpdate = json.load(f)
                                if not toUpdate['imdb'] == None:
                                        self.imdb = toUpdate['imdb']	
                                        self.update_imdbid = True
                        except:
                                pass
                        f.close()
                        try:
                               	xbmcvfs.delete(ADDON_TOUPDATE + self.type + '_' + str(self.id) + '.dump')
                        except:
				pass
		else:
			self.imdb = None
                if 'imdb' in getMovies['ratings']:
                        self.imdb_default = getMovies['ratings']['imdb']['default']
                        self.rating = ('%.1f' % round(getMovies['ratings']['imdb']['rating'], 1))
                        self.votes = getMovies['ratings']['imdb']['votes']
                else:
                        self.imdb_default = False
                        self.rating = '0.0'
                        self.votes = 0
                self.updateStatus = None
                self.ratingNew = ''
                self.votesNew = 0

        #generating VideoLibrary.SetMovieDetails
        def setDetails(self):
                movieDetails = dict(jsonrpc='2.0', method='VideoLibrary.SetMovieDetails', id=1)
                movieDetails['params'] = {}
                movieDetails['params'][self.type + 'id'] = self.id
                if self.imdb_default:
                        movieDetails['params']['rating'] = float(self.rating)
                        movieDetails['params']['votes'] = str(self.votes)
                else:
                        movieDetails['params']['ratings'] = {}
                        movieDetails['params']['ratings']['imdb'] = {}
                        movieDetails['params']['ratings']['imdb']['default'] = True
                        movieDetails['params']['ratings']['imdb']['rating'] = float(self.rating)
                        movieDetails['params']['ratings']['imdb']['votes'] = self.votes
                if hasattr(self, 'top250'):
                        movieDetails['params']['top250'] = self.top250
		if self.update_imdbid:
			movieDetails['params']['uniqueid'] = {}
			movieDetails['params']['uniqueid']['imdb'] = self.imdb
                return movieDetails

        def setDetailsToSave(self):
                movieDetails = self.setDetails()
                movieDetails['title'] = self.title
                if self.updateStatus:
                        movieDetails['updateStatus'] = self.updateStatus
                movieDetails['imdb'] = self.imdb
                return movieDetails
                
        #save file to failed directory
        def saveFailed(self):
                toWrite = self.setDetailsToSave()
                file = xbmcvfs.File(ADDON_FAILED + self.type + '_' + str(self.id) + '.dump', 'w')
		json.dump(toWrite, file, indent=2)
		file.close()

        def saveQueued(self):
                toWrite = self.setDetails()
                file = xbmcvfs.File(ADDON_QUEUED + self.type + '_' + str(self.id) + '.todb', 'w')
		json.dump(toWrite, file)
		file.close()

class JSONTVShowParser(JSONMovieParser):
        def __init__(self, getTVShows):
		if not hasattr(self, 'type'):
                	self.type = 'tvshow'
                JSONMovieParser.__init__(self, getTVShows)
                if 'tvdb' in getTVShows['uniqueid']:
			self.tvdb = getTVShows['uniqueid']['tvdb']
		elif 'unknown' in getTVShows['uniqueid'] and not getTVShows['uniqueid']['unknown'].startswith('tt'):
			self.tvdb = getTVShows['uniqueid']['unknown']
		else:
                        self.tvdb = None
                if self.type == 'tvshow':
                        self.episodesRating = {}
                        self.episodesVotes = {}

        def setDetails(self):
                tvshowDetails = JSONMovieParser.setDetails(self)
                tvshowDetails['method'] = 'VideoLibrary.SetTVShowDetails'
                return tvshowDetails

        def setDetailsToSave(self):
                tvshowDetails = JSONMovieParser.setDetailsToSave(self)
                tvshowDetails['tvdb'] = self.tvdb
                return tvshowDetails

class JSONEpisodeParser(JSONTVShowParser):
        def __init__(self, getEpisodes, tvshowid):
		if not hasattr(self, 'type'):
                	self.type = 'episode'
                JSONTVShowParser.__init__(self, getEpisodes)
		self.tvshowid = tvshowid
                self.tvshow_imdb = ''
                self.episode = getEpisodes['episode']
                self.season = getEpisodes['season']
       
        def setDetails(self):
                episodeDetails = JSONTVShowParser.setDetails(self)
                episodeDetails['method'] = 'VideoLibrary.SetEpisodeDetails'
                return episodeDetails
        
        def setDetailsToSave(self):
                episodeDetails = JSONTVShowParser.setDetailsToSave(self)
                episodeDetails['tvshowid'] = self.tvshowid
                episodeDetails['tvshow_imdb'] = self.tvshow_imdb
                episodeDetails['episode'] = self.episode
                episodeDetails['season'] = self.season
		return episodeDetails
            
def check_dirs():
        logger = Logger();
	# create dir in addon data if not exist
	if not xbmcvfs.exists(ADDON_PATH_DATA):
                logger.log("Addon directory not exist, creating.")
		xbmcvfs.mkdir(ADDON_PATH_DATA)

	for dir in [ADDON_FAILED, ADDON_QUEUED, ADDON_TOUPDATE]:
		if not xbmcvfs.exists(dir):
                	logger.log('%s directory not exist, creating.' % dir)
			xbmcvfs.mkdir(dir)
		else:
			if not dir == ADDON_TOUPDATE:
                		#removing files
                		dirs, files = xbmcvfs.listdir(dir)
                		for file in files:
                        		try:
                                		xbmcvfs.delete(dir + file)
                        		except:
                                		logger.log('Cant delete %s' % dir + file)

def readdb_movies(qqTasks, type):
       	logger = Logger()
	logger.threadName = threading.currentThread().getName()

        global countAll, countQueued, processingDB
        start = 0
        queued = 0
        
        readDB = dict(jsonrpc='2.0', method='VideoLibrary.GetMovies', id=1)
        readDB['params'] = {}
        readDB['params']['properties'] = ['uniqueid', 'title', 'ratings', 'top250']
        if type == 'tvshows':
                readDB['method'] = 'VideoLibrary.GetTVShows'
                readDB['params']['properties'] = ['uniqueid', 'title', 'ratings']
        readDB['params']['limits'] = {}

        while processingDB:
                readDB['params']['limits']['start'] = start
                start += 50
                readDB['params']['limits']['end'] = start
                respDB = kodiutils.kodi_json_request(readDB)
                if respDB:
                        if type in respDB:
                                if countAll == 0:
                                        countAll = respDB['limits']['total']
					if type == 'tvshows':
						readEpisode = dict(jsonrpc='2.0', method='VideoLibrary.GetEpisodes', id=1)
						readEpisode['params'] = {}
        					readEpisode['params']['limits'] = {}
						readEpisode['params']['limits']['start'] = 0
						readEpisode['params']['limits']['end'] = 1
				                respEpisode = kodiutils.kodi_json_request(readEpisode)
				                if respEpisode:
             				        	if 'episodes' in respEpisode:
								countAll += respEpisode['limits']['total']
                                for movie in respDB[type]:
                                        if type == 'movies':
                                                movieData = JSONMovieParser(movie)
                                        elif type == 'tvshows':
                                                movieData = JSONTVShowParser(movie)
                                        threadsLock.acquire()
                                        countQueued += 1
                                        qqTasks.put((100,movieData))
                                        threadsLock.release()
                                        queued += 1
                        else:
                                processingDB = False
                                logger.log('exiting after %d records queued' % queued)
                                
def readdb_episodes(qqTasks, qqChanged, qqEpisodes):
	logger = Logger()
	logger.threadName = threading.currentThread().getName()
        global countQueued, processingEpisodes
        counter = 0

        while True:
		if qqEpisodes.qsize() == 0:
			processingEpisodes = False
                tvshow = qqEpisodes.get()
                if exitThreads:
			qqEpisodes.task_done()
                        logger.log('exiting after %d tvshows processing' % counter)
                        break
                        
		processingEpisodes = True
                counter += 1
		logger.log('tvshowid %d (%s) reading episodes from DB' % (tvshow.id, tvshow.title))
                readDB = dict(jsonrpc='2.0', method='VideoLibrary.GetEpisodes', id=1)
                readDB['params'] = {}
                readDB['params']['tvshowid'] = tvshow.id
                readDB['params']['properties'] = ['episode', 'season', 'uniqueid', 'ratings']
                respDB = kodiutils.kodi_json_request(readDB)
                if respDB:
                        if 'episodes' in respDB:
                                threadsLock.acquire()
                                for episode in respDB['episodes']:
                                        episodeData = JSONEpisodeParser(episode, tvshow.id)
                                        episodeData.title = tvshow.title
                                        episodeData.tvshow_imdb = tvshow.imdb
                                        episodeNo = ('%dx%d' % (episodeData.season, episodeData.episode))
                                        if episodeNo in tvshow.episodesRating:
                                                episodeData.ratingNew = tvshow.episodesRating[episodeNo]
                                                if episodeNo in tvshow.episodesVotes:
                                                        episodeData.votesNew = tvshow.episodesVotes[episodeNo]
                                        countQueued += 1
                                        qqTasks.put((50,episodeData))
                                threadsLock.release()
		qqEpisodes.task_done()

def writedb_movies():
	logger = Logger()
	counter = 0
	dirs, files = xbmcvfs.listdir(ADDON_QUEUED)
        logger.log('Writings to DB from queued dir: %d records' % len(files))
	for file in files:
		counter += 1
                progress.update(counter*100/len(files), 'Writing database...',' ')
		if file.endswith('.todb'):
			f = xbmcvfs.File(ADDON_QUEUED + file, 'r')
                	toWrite = json.load(f)
                	f.close()
               		try:
				xbmcvfs.delete(ADDON_QUEUED + file)
               		except:
                       		logger.log('Cant delete %s' % ADDON_QUEUED + file)
                        kodiutils.kodi_json_request(toWrite)
			
def movie_thread(qqTasks, qqChanged, qqEpisodes):
	logger = Logger()
	logger.threadName = threading.currentThread().getName()
	counter = 0

	while True:
                movieData = qqTasks.get()[1]
                if exitThreads:
                        qqTasks.task_done()
                        logger.log('exiting after %d IMDB checking' % counter)
                        break

                counter += 1
		count = qqTasks.qsize()
                progress.update((countQueued - count)*100/countAll, 'Updating ratings...', movieData.title)

                isIMDbRating = False
		if movieData.type == 'episode':
                        if not movieData.ratingNew == '':
                                isIMDbRating = True
                                logger.log('episodeid %d (%s %dx%d) IMDb fast path' %
                                        (movieData.id, movieData.title, movieData.season, movieData.episode))
                        else:
                                logger.log('episodeid %d (%s %dx%d) checking IMDb %s' %
                                        (movieData.id, movieData.title, movieData.season, movieData.episode, movieData.imdb))
		else:
                	logger.log('%sid %d (%s) checking IMDb %s' %
                	        (movieData.type, movieData.id, movieData.title, movieData.imdb))
                if not isIMDbRating:
                        if not movieData.imdb:
                                logger.log('missing IMDb number')
                                movieData.saveFailed()
                        elif not smartimdb.get_imdb(movieData):
                                logger.log('IMDb failed status: %s' % movieData.updateStatus)
                                movieData.saveFailed()
                        else:
                                isIMDbRating = True
                if isIMDbRating:
                        update = False
                        if not movieData.ratingNew == '':
                                if not movieData.ratingNew == movieData.rating or (votesWithoutRating and movieData.votesNew > movieData.votes):
                                        update = True
                                        logger.log('rating changed %s -> %s (votes %d -> %d)' % (movieData.rating,
                                                movieData.ratingNew, movieData.votes, movieData.votesNew))
                                        movieData.rating = movieData.ratingNew
                                        movieData.votes = movieData.votesNew
                        if movieData.type == 'movie' and movieData.top250New > -1:
                                if not movieData.top250New == movieData.top250:
                                        update = True
                                        logger.log('top250 changed %d -> %d' % (movieData.top250, movieData.top250New))
                                        movieData.top250 = movieData.top250New
                        if update:
                                try:
                                	qqChanged.put_nowait(movieData)
                                except Queue.Full:
                                	movieData.saveQueued()
                        #add episodes to update
                        if movieData.type == 'tvshow':
				if not smartimdb.get_fast_ratings(movieData):
					logger.log('fast episodes ratings checking: %s' % movieData.updateStatus)
				qqEpisodes.put(movieData)
                qqTasks.task_done()

def writedb_thread(qqChanged):
	logger = Logger()
	logger.threadName = threading.currentThread().getName()
	counter = 0

        while True:
                movieData = qqChanged.get()
                if movieData:
                        counter += 1
                        kodiutils.kodi_json_request(movieData.setDetails())
	        qqChanged.task_done()
                if qqChanged.qsize() == 0 and exitThreads:
                        logger.log('exiting after %d DB writings' % counter)
                        break

def update_movies():
       	logger = Logger()
        global countAll, countQueued, exitThreads, processingDB, processingEpisodes

	#create queue for threads (objects and counting)
	qqTasks = Queue.PriorityQueue(100)
	qqChanged = Queue.Queue(10)
	qqEpisodes = Queue.Queue()

        #check dirs
        check_dirs()

	notify = TimeNotification()
        notify.show(ADDON_NAME, 'Starting ratings update')
        progress.create('Preparing for checking...')

        for type in toCheck:
                progress.update(0, 'Preparing %s for checking...' % type, ' ')

                #start threads
                notify.start()
                threads = []
                countAll = 0
                countQueued = 0
                exitThreads = False
                processingDB = True
                processingEpisodes = False

                for threadNo in range (0, ThreadsNumber):
                        thread = threading.Thread(name='%s_%d' % (type, threadNo), target=movie_thread,
                                args=(qqTasks, qqChanged, qqEpisodes))
                        thread.start()
                        threads.append(thread)

                thread = threading.Thread(name='writedb', target=writedb_thread, args=(qqChanged,))
                thread.start()

                if type == 'tvshows':
	                processingEpisodes = True
                        thread = threading.Thread(name='readdb_episodes', target=readdb_episodes, 
                                args=(qqTasks, qqChanged, qqEpisodes))
                        thread.start()

                thread = threading.Thread(name='readdb_%s' % type, target=readdb_movies, args=(qqTasks, type))
                thread.start()

                while processingDB or processingEpisodes:
                        if qqTasks.qsize() > 0:
				qqTasks.join()
			else:
				xbmc.sleep(500)

                notify.stop()
                notify.items = countAll
		typeInfo = type
		if type == 'tvshows':
			typeInfo += '/episodes'
                notify.showTime('%d %s' % (countAll, typeInfo),'Checked {time} ({speed})')
                exitThreads = True
                for thread in threads:
                        qqTasks.put((100,None))
                                
                if type == 'tvshows':
                        qqEpisodes.put(None)

                qqChanged.put(None)
		xbmc.sleep(500)

		activeThreads = threading.activeCount()
		while activeThreads > 1:
			logger.log('%d active thread(s), waiting' % activeThreads)
			xbmc.sleep(500)
			activeThreads = threading.activeCount()

        writedb_movies()
	progress.close()

def start():
        windowid = 10000
       	logger = Logger()
        if xbmc.getInfoLabel('Window(%d).Property(UptodateIMDbIsRunning)' % windowid) == 'True':
                logger.log('script already running')
        else:
                xbmcgui.Window(windowid).setProperty('UptodateIMDbIsRunning', 'True')
                update_movies()
		xbmcgui.Window(windowid).clearProperty('UptodateIMDbIsRunning')
