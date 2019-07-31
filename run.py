from storage.create_table import CreateTable
from spiders.movies_list import Spider
from spiders.page_detail import Page
from multiprocessing import Pool, Process
import threading

def main(page_start):
    spider = Spider(page_start)
    page = Page()
    t1 = threading.Thread(target=spider.run())
    t2 = threading.Thread(target=page.run())
    t1.start()
    t2.start()
    t1.join()
    t2.join()


if __name__ == '__main__':
    table = CreateTable()
    table.create()

    with Pool(4) as pool:
        pool.map(main, range(0, 500, 20))
