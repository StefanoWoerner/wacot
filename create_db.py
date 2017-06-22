import pymysql as sql
import config as cfg


def main():
    # connect to db server without specific db
    conn = sql.connect(host=cfg.HOST, port=cfg.PORT,
                       user=cfg.USER, passwd=cfg.PASSWD)
    cursor = conn.cursor()

    # create db
    query = ("CREATE DATABASE IF NOT EXISTS `{}`"
             "DEFAULT CHARACTER SET utf8 DEFAULT COLLATE utf8_general_ci;"
             ).format(cfg.DB)
    cursor.execute(query)
    conn.commit()
    conn.close()

    # connect to db
    conn = sql.connect(host=cfg.HOST, port=cfg.PORT,
                       user=cfg.USER, passwd=cfg.PASSWD, db=cfg.DB)
    cursor = conn.cursor()

    # create the article table
    query = ("CREATE TABLE IF NOT EXISTS `article`("
             "`id` mediumint(8) UNSIGNED NOT NULL, "
             "`title` varchar(255) DEFAULT NULL, "
             "`con_count_human` mediumint(8) UNSIGNED DEFAULT NULL, "
             "`con_count_total` mediumint(8) UNSIGNED DEFAULT NULL, "
             "PRIMARY KEY (`id`), "
             "INDEX (`con_count_human`), "
             "INDEX (`con_count_total`)"
             ") ENGINE=InnoDB DEFAULT CHARSET=utf8;")
    cursor.execute(query)

    # create the contributor table
    query = ("CREATE TABLE IF NOT EXISTS `contributor`("
             "`username` varchar(255) NOT NULL, "
             "`registered` boolean NOT NULL, "
             "`bot` boolean DEFAULT FALSE, "
             "`bot_in_name` boolean DEFAULT FALSE, "
             "`article_con_count` mediumint(8) UNSIGNED DEFAULT NULL, "
             "`category_con_count` mediumint(8) UNSIGNED DEFAULT NULL, "
             "PRIMARY KEY (`username`, `registered`), "
             "INDEX (`bot`), "
             "INDEX (`article_con_count`), "
             "INDEX (`category_con_count`)"
             ") ENGINE=InnoDB DEFAULT CHARSET=utf8;")
    cursor.execute(query)

    # create the revision table
    query = ("CREATE TABLE IF NOT EXISTS `revision`("
             "`id` int(11) UNSIGNED NOT NULL, "
             "`article` mediumint(8) UNSIGNED NOT NULL, "
             "`contributor` varchar(255) NOT NULL, "
             "`edit_time` datetime DEFAULT NULL, "
             "PRIMARY KEY(`id`), "
             "FOREIGN KEY (`article`) REFERENCES article(`id`) "
             "ON UPDATE CASCADE ON DELETE CASCADE, "
             "FOREIGN KEY (`contributor`) REFERENCES contributor(`username`) "
             "ON UPDATE CASCADE ON DELETE CASCADE, "
             "INDEX (`article`), "
             "INDEX (`contributor`)"
             ") ENGINE=InnoDB DEFAULT CHARSET=utf8;")
    cursor.execute(query)

    # create the contribution table
    query = ("CREATE TABLE IF NOT EXISTS `contribution`("
             "`article` mediumint(8) UNSIGNED NOT NULL, "
             "`contributor` varchar(255) NOT NULL, "
             "`edit_count` smallint(5) UNSIGNED DEFAULT NULL, "
             "`avg_edit_time` datetime DEFAULT NULL, "
             "PRIMARY KEY(`article`, `contributor`), "
             "FOREIGN KEY (`article`) REFERENCES article(`id`) "
             "ON UPDATE CASCADE ON DELETE CASCADE, "
             "FOREIGN KEY (`contributor`) REFERENCES contributor(`username`) "
             "ON UPDATE CASCADE ON DELETE CASCADE, "
             "INDEX (`article`), "
             "INDEX (`contributor`)"
             ") ENGINE=InnoDB DEFAULT CHARSET=utf8;")
    cursor.execute(query)

    # create the co-authorship table
    query = ("CREATE TABLE IF NOT EXISTS `co_authorship`("
             "`article1` mediumint(8) UNSIGNED NOT NULL, "
             "`article2` mediumint(8) UNSIGNED NOT NULL, "
             "`count_human` FLOAT UNSIGNED DEFAULT NULL, "
             "`count_total` FLOAT UNSIGNED DEFAULT NULL, "
             "PRIMARY KEY (`article1`, `article2`), "
             "FOREIGN KEY (`article1`) REFERENCES article(`id`) "
             "ON UPDATE CASCADE ON DELETE CASCADE, "
             "FOREIGN KEY (`article2`) REFERENCES article(`id`) "
             "ON UPDATE CASCADE ON DELETE CASCADE, "
             "INDEX (`count_human`), "
             "INDEX (`count_total`)"
             ") ENGINE=InnoDB DEFAULT CHARSET=utf8;")
    cursor.execute(query)

    # create the category table
    query = ("CREATE TABLE IF NOT EXISTS `category` ("
             "`id` mediumint(8) UNSIGNED NOT NULL, "
             "`title` varchar(255) NOT NULL DEFAULT '', "
             "`pages` int(11) NOT NULL DEFAULT '0', "
             "`subcats` int(11) NOT NULL DEFAULT '0', "
             "`con_count_human` mediumint(8) UNSIGNED DEFAULT NULL, "
             "`con_count_total` mediumint(8) UNSIGNED DEFAULT NULL, "
             "PRIMARY KEY (`id`), "
             "UNIQUE KEY (`title`), "
             "INDEX (`con_count_human`), "
             "INDEX (`con_count_total`)"
             ") ENGINE=InnoDB DEFAULT CHARSET=utf8;")
    cursor.execute(query)

    # create the categorylink table
    query = ("CREATE TABLE IF NOT EXISTS `categorylink` ("
             "`article` mediumint(8) UNSIGNED NOT NULL, "
             "`category` varchar(255) NOT NULL, "
             "PRIMARY KEY (`article`,`category`), "
             "FOREIGN KEY (`article`) REFERENCES article(`id`) "
             "ON UPDATE CASCADE ON DELETE CASCADE, "
             "FOREIGN KEY (`category`) REFERENCES category(`title`) "
             "ON UPDATE CASCADE ON DELETE CASCADE"
             ") ENGINE=InnoDB DEFAULT CHARSET=utf8;")
    cursor.execute(query)

    # create the category_contribution table
    query = ("CREATE TABLE IF NOT EXISTS `category_contribution` ("
             "`category` mediumint(8) UNSIGNED NOT NULL, "
             "`contributor` varchar(255) NOT NULL, "
             "`article_count` smallint(5) UNSIGNED DEFAULT NULL, "
             "PRIMARY KEY(`category`, `contributor`), "
             "FOREIGN KEY (`category`) REFERENCES category(`id`) "
             "ON UPDATE CASCADE ON DELETE CASCADE, "
             "FOREIGN KEY (`contributor`) REFERENCES contributor(`username`) "
             "ON UPDATE CASCADE ON DELETE CASCADE, "
             "INDEX (`category`), "
             "INDEX (`contributor`)"
             ") ENGINE=InnoDB DEFAULT CHARSET=utf8;")
    cursor.execute(query)

    # create the category_co_authorship table
    query = ("CREATE TABLE IF NOT EXISTS `category_co_authorship` ("
             "`category1` mediumint(8) UNSIGNED NOT NULL, "
             "`category2` mediumint(8) UNSIGNED NOT NULL, "
             "`count_human` float UNSIGNED DEFAULT NULL, "
             "`count_total` float UNSIGNED DEFAULT NULL, "
             "PRIMARY KEY (`category1`,`category2`), "
             "FOREIGN KEY (`category1`) REFERENCES category(`id`) "
             "ON UPDATE CASCADE ON DELETE CASCADE, "
             "FOREIGN KEY (`category2`) REFERENCES category(`id`) "
             "ON UPDATE CASCADE ON DELETE CASCADE"
             ") ENGINE=InnoDB DEFAULT CHARSET=utf8;")
    cursor.execute(query)

    conn.commit()
    conn.close()
    print("Finished creating database.")

if (__name__ == "__main__"):
    main()
