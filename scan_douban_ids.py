#encoding=gbk
'''
    my[at]lijiejie.com
    http://www.lijiejie.com
'''
import httplib  
import MySQLdb as mdb
import threading
import Queue
import time

headers = {'User-Agent': 'Googlebot/2.1 (+http://www.google.com/bot.html)',
           'Connection': 'keep-alive'}

dbconn = mdb.Connection(host = '127.0.0.1', user = 'user', passwd = '******', db = 'dbname', charset='utf8')
cursor = dbconn.cursor()
id_queue = Queue.Queue(100)
lock = threading.Lock()

def check_album_id():
    global headers, dbconn, cursor, lock, id_queue
    while True:
        albumid = id_queue.get()
        print 'test album id', albumid 
        if albumid == None:
            id_queue.task_done()
            break
            return
        conn = httplib.HTTPConnection("www.douban.com")
        conn.request(method='GET', url='/subject/' + str(albumid) + '/', headers=headers)
        response = conn.getresponse()
        location = response.getheader('location', '')
        if location.find('music.douban.com') > 0:
            location = location.replace('http://music.douban.com/subject/', '')
            location = location.replace('/', '')
            lock.acquire()
            cursor.execute("select * from album where album_url = %s", location)
            if cursor.fetchone() == None:
                cursor.execute('INSERT INTO album(album_url) values (%s)', location)
                dbconn.commit()
                print 'add album url', location
            lock.release()
        conn.close()
        id_queue.task_done()
        time.sleep(0.01)


all_threads = []
for i in range(50):
    new_thread = threading.Thread(target=check_album_id)
    new_thread.start()
    all_threads.append(new_thread)

for j in range(1, 30000000):
    id_queue.put(j)

for j in range(50):
    id_queue.put(None)

id_queue.join()
for thread in all_threads:
    thread.join()

dbconn.close()
print 'All Done.'
