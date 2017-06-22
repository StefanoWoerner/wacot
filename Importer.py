import DBInterface
import config as cfg
import xml.etree.ElementTree as ET
import logging
from time import strftime, strptime


class Importer:

    # init logging
    logging.basicConfig(filename='wdir/import.log')

    time_format_in = '%Y-%m-%dT%H:%M:%SZ'
    time_format_out = '%Y-%m-%d %H:%M:%S'

    def __init__(self, dbi=None, xml_filename=cfg.IMPORT_XML_FILEPATH,
                 xml_ns=cfg.IMPORT_XML_NS,
                 category_filename=cfg.IMPORT_CATEGORY_FILEPATH,
                 categorylink_filename=cfg.IMPORT_CATEGORYLINK_FILEPATH):
        if dbi is None:
            self.__dbi = DBInterface.DBInterface()
        else:
            self.__dbi = dbi
        self.xml_filename = xml_filename
        self.xml_ns = xml_ns
        self.category_filename = category_filename
        self.categorylink_filename = categorylink_filename

    # method to print the status of import to the console
    def print_status(self, narticles, ncontributors, nrevisions, npages):
        print("{} articles, {} contributors, {} revisions in {} pages".format(
            narticles, ncontributors, nrevisions, npages))

    # method to to store all new collected objects to the database
    def add_to_db(self, articles, contributors, revisions):
        self.__dbi.add_articles(articles)
        self.__dbi.add_contributors(contributors)
        self.__dbi.add_revisions(revisions)

    def parse_revision(self, article_id, revision):
        c = revision.find(self.xml_ns + "contributor")
        rev_id = revision.find(self.xml_ns + "id")
        time = revision.find(self.xml_ns + "timestamp")
        if (c is not None and rev_id is not None and article_id is not None
                and time is not None):
            con = None
            rev = None
            time_text = strftime(self.time_format_out,
                                 strptime(time.text, self.time_format_in))
            # decide if the username or the IP will be used
            username = c.find(self.xml_ns + "username")
            ip = c.find(self.xml_ns + "ip")
            if (username is not None and
                    username.text is not None):
                # decide if the contributor is a bot
                if ("bot" in username.text.lower()):
                    con = (username.text, True, True)
                else:
                    con = (username.text, True, False)
                rev = (rev_id.text, article_id.text, username.text, time_text)
            elif (ip is not None and ip.text is not None):
                con = (ip.text, False, False)
                rev = (rev_id.text, article_id.text, ip.text, time_text)
            return (con, rev)
        else:
            # log a warning
            logging.warning("something is wrong with the revision")

    def import_xml(self):
        print("Importing from XML...")

        # initialize counters and collections
        page_count = 0
        num_articles = 0
        new_articles = []
        num_contributors = 0
        new_contributors = set()
        old_contributors = set()
        num_revisions = 0
        new_revisions = []

        # iteratively parse the XML file
        for event, elem in ET.iterparse(self.xml_filename):
            if (elem.tag == self.xml_ns + "page"):
                page_count += 1
                # print the status and store new objects to the database after
                # every thousand pages
                if (page_count % 1000 == 0):
                    num_articles += len(new_articles)
                    contributors_to_add = new_contributors.difference(
                        old_contributors)
                    num_contributors += len(contributors_to_add)
                    num_revisions += len(new_revisions)
                    self.add_to_db(
                        new_articles, contributors_to_add, new_revisions)
                    new_articles = []
                    old_contributors = old_contributors.union(new_contributors)
                    new_contributors = set()
                    new_revisions = []
                    self.print_status(num_articles, num_contributors,
                                      num_revisions, page_count)

                # only process pages, if ns = 0, i.e. the page is an article
                ns = elem.find(self.xml_ns + "ns")
                if (ns is not None and ns.text == "0"):
                    article_id = elem.find(self.xml_ns + "id")
                    title = elem.find(self.xml_ns + "title")
                    if (article_id is not None and article_id.text is not None
                            and title is not None and title.text is not None):
                        new_articles.append((article_id.text, title.text))

                        # iteratively look at the revisions of the page
                        for revision in elem.iter(self.xml_ns + "revision"):
                            (con, rev) = self.parse_revision(
                                article_id, revision)
                            if con is not None and rev is not None:
                                new_contributors.add(con)
                                new_revisions.append(rev)
                            # clear revision element to free memory
                            revision.clear()

                # clear page element to free memory
                elem.clear()

        num_articles += len(new_articles)
        contributors_to_add = new_contributors.difference(old_contributors)
        num_contributors += len(contributors_to_add)
        num_revisions += len(new_revisions)
        self.print_status(num_articles, num_contributors,
                          num_revisions, page_count)
        self.add_to_db(new_articles, contributors_to_add, new_revisions)
        print("Import from XML successful!")

    def import_categories(self):
        print("Importing from category and categorylink files...")
        print("categories...")
        with open(self.category_filename, 'r', encoding='utf8') as dump:
            lines = [l for l in dump.readlines() if l.startswith('INSERT')]
            i = 0
            for line in lines:
                i += 1
                print("Processing line {} of {}".format(i, len(lines)))
                values = [(x[0], x[1], x[2], x[3]) for x in
                          eval('[' + line[30:-2] + ']')]
                self.__dbi.add_categories(values)

        print("categorylinks...")
        with open(self.categorylink_filename, 'r', encoding='utf8') as dump:
            lines = [l for l in dump.readlines() if l.startswith('INSERT')]
            i = 0
            for line in lines:
                i += 1
                print("Processing line {} of {}".format(i, len(lines)))
                values = [(x[0], x[1]) for x in
                          eval('[' + line[35:-2] + ']')]
                self.__dbi.add_categorylinks(values)

        print("Import of categories and categorylinks successful!")
