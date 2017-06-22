import pymysql as sql
import config as cfg


class DBInterface:

    _conn = None

    def __init__(self, host=cfg.HOST, port=cfg.PORT, user=cfg.USER,
                 passwd=cfg.PASSWD, db=cfg.DB):
        # connect to db and get cursor
        self._conn = sql.connect(host=host, port=port, user=user,
                                 passwd=passwd, db=db, charset='utf8')
        self._cursor = self._conn.cursor()

    def __del__(self):
        if (self._conn is not None):
            self._conn.close()

    # method for bulk inserting multiple articles
    def add_articles(self, articles):
        count = 0
        querybase = "INSERT INTO article (id, title) VALUES "
        query = querybase
        for (id, title) in articles:
            id = self._conn.escape(id)
            title = self._conn.escape(title)
            query += "({}, {})".format(id, title)
            count += 1
            # finish and execute a query every thousand articles
            if (count % 1000 == 0):
                query += " ON DUPLICATE KEY UPDATE id=id;"
                self.execute_query(query)
                query = querybase
            else:
                query += ", "
        if (count % 1000 != 0):
            query = query[:-2]
            query += " ON DUPLICATE KEY UPDATE id=id;"
            self.execute_query(query)

    # method for bulk inserting multiple contributors
    def add_contributors(self, contributors):
        count = 0
        querybase = ("INSERT INTO contributor "
                     "(username, registered, bot_in_name) VALUES ")
        query = querybase
        for (username, registered, bot_in_name) in contributors:
            username = self._conn.escape(username)
            if (username is None):
                raise Exception(
                    "username and ip of contributor are both None!")
            query += "({}, {}, {})".format(username, registered, bot_in_name)
            count += 1
            # finish and execute a query every thousand contributors
            if (count % 1000 == 0):
                query += " ON DUPLICATE KEY UPDATE username=username;"
                self.execute_query(query)
                query = querybase
            else:
                query += ", "
        if (count % 1000 != 0):
            query = query[:-2]
            query += " ON DUPLICATE KEY UPDATE username=username;"
            self.execute_query(query)

    # method for bulk inserting multiple revisions
    def add_revisions(self, revisions):
        count = 0
        querybase = ("INSERT INTO revision "
                     "(id, article, contributor, edit_time) VALUES ")
        query = querybase
        for (id, article, contributor, edit_time) in revisions:
            id = self._conn.escape(id)
            article = self._conn.escape(article)
            contributor = self._conn.escape(contributor)
            edit_time = self._conn.escape(edit_time)
            query += "({}, {}, {}, {})".format(id,
                                               article, contributor, edit_time)
            count += 1
            # finish and execute a query every thousand contributions
            if (count % 1000 == 0):
                query += " ON DUPLICATE KEY UPDATE id=id;"
                self.execute_query(query)
                query = querybase
            else:
                query += ", "
        if (count % 1000 != 0):
            query = query[:-2]
            query += " ON DUPLICATE KEY UPDATE id=id;"
            self.execute_query(query)

    # method for bulk inserting multiple contributions
    def add_contributions(self, contributions):
        count = 0
        querybase = "INSERT INTO contribution (article, contributor) VALUES "
        query = querybase
        for (article, contributor) in contributions:
            article = self._conn.escape(article)
            contributor = self._conn.escape(contributor)
            query += "({}, {})".format(article, contributor)
            count += 1
            # finish and execute a query every thousand contributions
            if (count % 1000 == 0):
                query += " ON DUPLICATE KEY UPDATE article=article;"
                self.execute_query(query)
                query = querybase
            else:
                query += ", "
        if (count % 1000 != 0):
            query = query[:-2]
            query += " ON DUPLICATE KEY UPDATE article=article;"
            self.execute_query(query)

    # import categorylinks
    def add_categorylinks(self, values):
        self.execute_query("SET FOREIGN_KEY_CHECKS = 0;")
        count = 0
        querybase = "INSERT INTO categorylink (article, category) VALUES "
        query = querybase
        for value in values:
            query += "({}, {})".format(self._conn.escape(
                value[0]), self._conn.escape(value[1]))
            count += 1
            # finish and execute a query every thousand contributions
            if (count % 1000 == 0):
                query += " ON DUPLICATE KEY UPDATE article=article;"
                self.execute_query(query)
                query = querybase
            else:
                query += ", "
        if (count % 1000 != 0):
            query = query[:-2]
            query += " ON DUPLICATE KEY UPDATE article=article;"
            self.execute_query(query)

        self.execute_query("DELETE FROM categorylink WHERE article NOT IN "
                           "(SELECT id FROM article);")
        self.execute_query("SET FOREIGN_KEY_CHECKS = 1;")

    # import categories
    def add_categories(self, values):
        count = 0
        querybase = "INSERT INTO category (id, title, pages, subcats) VALUES "
        query = querybase
        for value in values:
            query += "({}, {}, {}, {})".format(
                self._conn.escape(value[0]), self._conn.escape(value[1]),
                self._conn.escape(value[2]), self._conn.escape(value[3]))
            count += 1
            # finish and execute a query every thousand contributions
            if (count % 1000 == 0):
                query += " ON DUPLICATE KEY UPDATE id=id;"
                self.execute_query(query)
                query = querybase
            else:
                query += ", "
        if (count % 1000 != 0):
            query = query[:-2]
            query += " ON DUPLICATE KEY UPDATE id=id;"
            self.execute_query(query)

    # method for executing raw queries and directly committing
    def execute_query(self, query):
        self._cursor.execute(query)
        self._conn.commit()
