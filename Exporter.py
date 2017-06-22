import DBInterface
import config as cfg
import csv
import html


class Exporter:

    def __init__(self, dbi=None, num_edges=cfg.BEST_EXPORT_NUM_EDGES,
                 denominator_power=cfg.SCORE_DENOMINATOR_POWER,
                 node_file=cfg.BEST_EXPORT_NODE_FILE,
                 edge_file=cfg.BEST_EXPORT_EDGE_FILE,
                 graph_file=cfg.BEST_EXPORT_GRAPH_FILE):
        if dbi is None:
            self.__dbi = DBInterface.DBInterface()
        else:
            self.__dbi = dbi
        self.num_edges = num_edges
        self.denominator_power = denominator_power
        self.node_file = node_file
        self.edge_file = edge_file
        self.graph_file = graph_file

    def export_article_similarities_to_graphml(self):
        print("Selecting best article similarities...")
        (nodes, edges) = self.__get_best_article_similarities()
        print("Writing file...")
        self.__write_graphml(nodes, edges)
        print("Done.")

    def export_article_similarities_to_csv(self):
        print("Selecting best article similarities...")
        (nodes, edges) = self.__get_best_article_similarities()
        print("Writing files...")
        self.__write_csv(nodes, edges)
        print("Done.")

    def export_category_similarities_to_graphml(self):
        print("Selecting best category similarities...")
        (nodes, edges) = self.__get_best_category_similarities()
        print("Writing file...")
        self.__write_graphml(nodes, edges)
        print("Done.")

    def export_category_similarities_to_csv(self):
        print("Selecting best category similarities...")
        (nodes, edges) = self.__get_best_category_similarities()
        print("Writing files...")
        self.__write_csv(nodes, edges)
        print("Done.")

    def __get_best_article_similarities(self):

        q = ("SELECT a.id, a.title, b.id, b.title, "
             "(count_human / pow(a.con_count_human + b.con_count_human, {})) "
             "AS score FROM co_authorship "
             "JOIN article AS a ON article1 = a.id "
             "JOIN article AS b ON article2 = b.id "
             "WHERE count_human != 0 ORDER BY score DESC LIMIT {};".format(
                 self.denominator_power, self.num_edges))

        # get the query result
        self.__dbi.execute_query(q)
        rows = self.__dbi._cursor.fetchall()

        # initialize collections
        # a set is used for the nodes for easy deduplication
        nodes = set()
        edges = []

        # put the data in the collections
        for (id1, title1, id2, title2, score) in rows:
            nodes.add((id1, title1))
            nodes.add((id2, title2))
            edges.append((id1, id2, score))

        return (sorted(nodes), edges)

    def __get_best_category_similarities(self):

        q = ("SELECT a.id, a.title, b.id, b.title, "
             "(count_human / pow(a.con_count_human + b.con_count_human, {})) "
             "AS score FROM category_co_authorship "
             "JOIN category AS a ON category1 = a.id "
             "JOIN category AS b ON category2 = b.id "
             "WHERE count_human != 0 ORDER BY score DESC LIMIT {};".format(
                 self.denominator_power, self.num_edges))

        # get the query result
        self.__dbi.execute_query(q)
        rows = self.__dbi._cursor.fetchall()

        # initialize collections
        # a set is used for the nodes for easy deduplication
        nodes = set()
        edges = []

        # put the data in the collections
        for (id1, title1, id2, title2, score) in rows:
            nodes.add((id1, title1))
            nodes.add((id2, title2))
            edges.append((id1, id2, score))

        return (sorted(nodes), edges)

    def __write_graphml(self, nodes, edges):
        doc_head = ('<?xml version="1.0" encoding="UTF-8"?>\n'
                    '<graphml xmlns="http://graphml.graphdrawing.org/xmlns">\n'
                    '  <key attr.name="Title" attr.type="string" '
                    'for="node" id="title"/>\n'
                    '  <key attr.name="Similarity score" attr.type="double" '
                    'for="edge" id="similarity"/>\n'
                    '  <graph edgedefault="undirected">\n')
        node_template = ('    <node id="{}">\n'
                         '      <data key="title">{}</data>\n'
                         '    </node>\n')
        edge_template = ('    <edge source="{}" target="{}">\n'
                         '      <data key="similarity">{}</data>\n'
                         '    </edge>\n')
        doc_foot = ('  </graph>\n'
                    '</graphml>\n')

        with open(self.graph_file, "w") as graphml_file:
            graphml_file.write(doc_head)
            for (id, title) in nodes:
                graphml_file.write(
                    node_template.format(id, html.escape(title)))
            for (source, target, similarity) in edges:
                graphml_file.write(edge_template.format(
                    source, target, similarity))
            graphml_file.write(doc_foot)

    def __write_csv(self, nodes, edges):
        # write node file
        with open(self.node_file, "w") as csv_file:
            csv_writer = csv.writer(csv_file, delimiter='\t')
            csv_writer.writerow(("id", "label"))
            csv_writer.writerows(nodes)

        # write edge file
        with open(self.edge_file, "w") as csv_file:
            csv_writer = csv.writer(csv_file, delimiter='\t')
            csv_writer.writerow(("source", "target", "weight"))
            csv_writer.writerows(edges)
