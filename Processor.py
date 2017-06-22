import DBInterface
import config as cfg


class Processor:

    def __init__(self, dbi=None,
                 update=cfg.COAUTH_UPDATE,
                 limit=cfg.COAUTH_LIMIT,
                 interval_size=cfg.COAUTH_INTERVAL_SIZE,
                 initial_interval_size=cfg.COAUTH_INITIAL_INTERVAL_SIZE,
                 initial_offset=cfg.COAUTH_INITIAL_OFFSET,
                 count_power=cfg.COAUTH_COUNT_POWER,
                 time_power=cfg.COAUTH_TIME_POWER):
        if dbi is None:
            self.__dbi = DBInterface.DBInterface()
        else:
            self.__dbi = dbi
        self.update = update
        self.limit = limit
        self.interval_size = interval_size
        self.initial_interval_size = initial_interval_size
        self.initial_offset = initial_offset
        self.count_power = count_power
        self.time_power = time_power

    # method to print the status of import to the console
    def print_status(self, upper, limit, upper_id, pair_count):
        msg = ("{} of {} objects processed (up to ID {}), {} pairs "
               "created/updated")
        print(msg.format(upper, limit, upper_id, pair_count))

    def generate_article_co_authorship(self):

        print('generating article co-authorship...')

        # Query Prototype
        # query_t = ("INSERT INTO co_authorship(article1, article2, count_human) "
        #           "SELECT t.article1, t.article2, t.ct FROM "
        #           "(SELECT con1.article as article1, con2.article as article2, "
        #           "count(*) as ct FROM contribution as con1 "
        #           "JOIN contribution as con2 ON con1.contributor=con2.contributor "
        #           "AND con1.article < con2.article "
        #           "JOIN contributor as c ON con1.contributor=c.username "
        #           "WHERE c.bot = 0 AND c.con_count < " + human_bot_threshold + " "
        #           "AND con1.article <= {} AND con2.article <= {} "
        #           "GROUP BY con1.article, con2.article HAVING ct > 0) as t "
        #           "ON DUPLICATE KEY UPDATE count_human = t.ct;")

        outer_start = ("INSERT INTO co_authorship"
                       "(article1, article2, count_human) ")
        update_outer_start = "SELECT t.article1, t.article2, t.ct FROM ("
        inner_start = ("SELECT con1.article AS article1, con2.article AS article2, "
                       "sum(pow(least(con1.edit_count, con2.edit_count), {}) / "
                       "(POW(ABS(DATEDIFF(con1.avg_edit_time, con2.avg_edit_time)), {}) + 1)) AS ct "
                       "FROM contribution AS con1 "
                       "JOIN contribution AS con2 "
                       "ON con1.contributor = con2.contributor "
                       "AND con1.article < con2.article "
                       "JOIN contributor AS c ON con1.contributor=c.username "
                       "WHERE c.bot = 0 ".format(self.count_power, self.time_power))
        inner_1leq_2leq = "AND con1.article <= {} AND con2.article <= {} "
        inner_2greater = "AND con2.article > {} "
        inner_end = "GROUP BY con1.article, con2.article HAVING ct > 0"
        update_outer_end = ") AS t ON DUPLICATE KEY UPDATE count_human = t.ct;"
        ignore_outer_end = " ON DUPLICATE KEY UPDATE article1 = article1;"

        if (self.update):
            query_start = outer_start + update_outer_start + inner_start
            query_end = inner_end + update_outer_end
        else:
            query_start = outer_start + inner_start
            query_end = inner_end + ignore_outer_end

        q = ("SELECT count(*) FROM article")
        self.__dbi.execute_query(q)
        actual_limit = min(
            self.limit - 1, self.__dbi._cursor.fetchone()[0] - 1)
        pair_count = 0

        if (self.initial_offset == 0):
            q = ("SELECT id FROM article ORDER BY id LIMIT 1 OFFSET {}")
            self.__dbi.execute_query(q.format(self.initial_interval_size))
            upper_id = self.__dbi._cursor.fetchone()[0]
            query = query_start
            query += inner_1leq_2leq.format(upper_id, upper_id)
            query += query_end
            self.__dbi.execute_query(query)
            offset = self.initial_interval_size
            pair_count += self.__dbi._cursor.rowcount
            self.print_status(self.initial_interval_size,
                              actual_limit, upper_id, pair_count)
        else:
            offset = self.initial_offset
            q = ("SELECT id FROM article ORDER BY id LIMIT 1 OFFSET {}")
            self.__dbi.execute_query(q.format(self.initial_offset))
            upper_id = self.__dbi._cursor.fetchone()[0]

        lower_id = upper_id

        while offset < actual_limit:
            new_upper = min(offset + self.interval_size, actual_limit)
            q = ("SELECT id FROM article ORDER BY id LIMIT 1 OFFSET {}")
            self.__dbi.execute_query(q.format(new_upper))
            upper_id = self.__dbi._cursor.fetchone()[0]

            query = query_start
            query += inner_1leq_2leq.format(upper_id, upper_id)
            query += inner_2greater.format(lower_id)
            query += query_end

            self.__dbi.execute_query(query)
            offset += self.interval_size
            pair_count += self.__dbi._cursor.rowcount
            self.print_status(new_upper, actual_limit, upper_id, pair_count)
            lower_id = upper_id

        print('done!')

    def generate_category_co_authorship(self):

        print('generating category co-authorship...')

        outer_start = ("INSERT INTO category_co_authorship"
                       "(category1, category2, count_human) ")
        update_outer_start = "SELECT t.category1, t.category2, t.ct FROM ("
        inner_start = ("SELECT con1.category AS category1, con2.category AS category2, "
                       "sum(pow(least(con1.article_count, con2.article_count), {})) AS ct "
                       "FROM category_contribution AS con1 "
                       "JOIN category_contribution AS con2 "
                       "ON con1.contributor = con2.contributor "
                       "AND con1.category < con2.category "
                       "JOIN contributor AS c ON con1.contributor = c.username "
                       "WHERE c.bot = 0 ".format(self.count_power))
        inner_1leq_2leq = "AND con1.category <= {} AND con2.category <= {} "
        inner_2greater = "AND con2.category > {} "
        inner_end = "GROUP BY con1.category, con2.category HAVING ct > 0"
        update_outer_end = ") AS t ON DUPLICATE KEY UPDATE count_human = t.ct;"
        ignore_outer_end = " ON DUPLICATE KEY UPDATE category1 = category1;"

        if (self.update):
            query_start = outer_start + update_outer_start + inner_start
            query_end = inner_end + update_outer_end
        else:
            query_start = outer_start + inner_start
            query_end = inner_end + ignore_outer_end

        q = ("SELECT count(*) FROM category")
        self.__dbi.execute_query(q)
        actual_limit = min(
            self.limit - 1, self.__dbi._cursor.fetchone()[0] - 1)
        pair_count = 0

        if (self.initial_offset == 0):
            q = ("SELECT id FROM category ORDER BY id LIMIT 1 OFFSET {}")
            self.__dbi.execute_query(q.format(self.initial_interval_size))
            upper_id = self.__dbi._cursor.fetchone()[0]
            query = query_start
            query += inner_1leq_2leq.format(upper_id, upper_id)
            query += query_end
            self.__dbi.execute_query(query)
            offset = self.initial_interval_size
            pair_count += self.__dbi._cursor.rowcount
            self.print_status(self.initial_interval_size,
                              actual_limit, upper_id, pair_count)
        else:
            offset = self.initial_offset
            q = ("SELECT id FROM category ORDER BY id LIMIT 1 OFFSET {}")
            self.__dbi.execute_query(q.format(self.initial_offset))
            upper_id = self.__dbi._cursor.fetchone()[0]

        lower_id = upper_id

        while offset < actual_limit:
            new_upper = min(offset + self.interval_size, actual_limit)
            q = ("SELECT id FROM category ORDER BY id LIMIT 1 OFFSET {}")
            self.__dbi.execute_query(q.format(new_upper))
            upper_id = self.__dbi._cursor.fetchone()[0]

            query = query_start
            query += inner_1leq_2leq.format(upper_id, upper_id)
            query += inner_2greater.format(lower_id)
            query += query_end

            self.__dbi.execute_query(query)
            offset += self.interval_size
            pair_count += self.__dbi._cursor.rowcount
            self.print_status(new_upper, actual_limit, upper_id, pair_count)
            lower_id = upper_id

        print('done!')
