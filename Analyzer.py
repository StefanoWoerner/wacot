import DBInterface
import config as cfg
import math


class Analyzer:

    def __init__(self, dbi=None, human_bot_threshold=cfg.HUMAN_BOT_THRESHOLD):
        if dbi is None:
            self.__dbi = DBInterface.DBInterface()
        else:
            self.__dbi = dbi
        self.human_bot_threshold = human_bot_threshold

    def compute_article_contributions(self):
        query = ("INSERT INTO contribution"
                 "(article, contributor, edit_count, avg_edit_time) "
                 "SELECT t.article, t.contributor, t.ct, t.avg_time FROM ("
                 "SELECT article, contributor, count(*) as ct, "
                 "FROM_UNIXTIME(AVG(UNIX_TIMESTAMP(edit_time))) as avg_time "
                 "FROM revision GROUP BY article, contributor) as t "
                 "ON DUPLICATE KEY UPDATE edit_count = t.ct, "
                 "avg_edit_time = t.avg_time;")

        print('computing article contributions from revisions...')
        self.__dbi.execute_query(query)
        print('done!')

    def compute_category_contributions(self):
        query_t = ("INSERT INTO category_contribution"
                   "(category, contributor, article_count) "
                   "SELECT t.category, t.contributor, t.ct FROM ("
                   "SELECT cat.id AS category, contributor, count(*) AS ct "
                   "FROM categorylink AS catl JOIN contribution AS con "
                   "ON con.article=catl.article "
                   "JOIN (SELECT * FROM category ORDER BY id "
                   "LIMIT {} OFFSET {}) AS cat "
                   "ON catl.category = cat.title "
                   "GROUP BY catl.category, con.contributor) AS t "
                   "ON DUPLICATE KEY UPDATE article_count = t.ct;")

        print('computing category contributions from article contributions...')
        q = ("SELECT count(*) FROM category")
        self.__dbi.execute_query(q)
        num_cat = self.__dbi._cursor.fetchone()[0]

        interval_size = 1000
        for i in range(0, math.ceil(num_cat / interval_size)):
            q = query_t.format(interval_size, interval_size * i)
            print('{} to {} of {} categories processed'.format(
                interval_size * i, interval_size * (i + 1), num_cat))
            self.__dbi.execute_query(q)
        print('done!')

    def compute_bot_flags(self):
        print('updating contribution counts...')

        print("article_con_count in contributor")
        query = ("UPDATE contributor SET article_con_count = "
                 "(SELECT count(*) as ct FROM contribution as e "
                 "WHERE e.contributor = contributor.username);")
        self.__dbi.execute_query(query)

        print("category_con_count in contributor")
        query = ("UPDATE contributor SET category_con_count = "
                 "(SELECT count(*) as ct FROM contribution as e "
                 "WHERE e.contributor = contributor.username);")
        self.__dbi.execute_query(query)

        print('computing bot flags...')
        query = ("UPDATE contributor SET bot = "
                 "(bot_in_name OR article_con_count >= {});".format(
                     self.human_bot_threshold))
        self.__dbi.execute_query(query)

        print('done!')

    def count_article_contributions(self):
        print('updating contribution counts...')

        print("con_count_human in article")
        query = ("UPDATE article SET con_count_human = "
                 "(SELECT count(*) as ct FROM contribution as e "
                 "JOIN contributor as c ON e.contributor=c.username "
                 "WHERE e.article = article.id and c.bot = 0);")
        self.__dbi.execute_query(query)

        print("con_count_total in article")
        query = ("UPDATE article SET con_count_total = "
                 "(SELECT count(*) as ct FROM contribution as e "
                 "WHERE e.article = article.id);")
        self.__dbi.execute_query(query)

        print('done!')

    def count_category_contributions(self):
        print('updating contribution counts...')

        print("con_count_human in category")
        query = ("UPDATE category SET con_count_human = "
                 "(SELECT count(*) as ct FROM category_contribution as e "
                 "JOIN contributor as c ON e.contributor=c.username "
                 "WHERE e.category = category.id and c.bot = 0);")
        self.__dbi.execute_query(query)

        print("con_count_total in category")
        query = ("UPDATE category SET con_count_total = "
                 "(SELECT count(*) as ct FROM category_contribution as e "
                 "WHERE e.category = category.id);")
        self.__dbi.execute_query(query)

        print('done!')
