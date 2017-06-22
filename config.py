# ========================================================== #
# Configuration for the wikipedia article co-authorship tool #
# ========================================================== #

# database connection
# -----------------------------------------------------------------------------
HOST = 'localhost'
PORT = 3306
USER = 'wacot'
PASSWD = 'wacotpass123'
DB = 'sw_test'
# -----------------------------------------------------------------------------


# import from XML/SQL dump
# -----------------------------------------------------------------------------
# XML file for import
IMPORT_XML_FILEPATH = 'wdir/simplewiki-20170620-stub-meta-history.xml'
# XML base namespace
IMPORT_XML_NS = '{http://www.mediawiki.org/xml/export-0.10/}'
# SQL category file for import
IMPORT_CATEGORY_FILEPATH = 'wdir/simplewiki-20170620-category.sql'
# SQL categorylink file for import
IMPORT_CATEGORYLINK_FILEPATH = 'wdir/simplewiki-20170620-categorylinks.sql'
# -----------------------------------------------------------------------------

# Numbers
# -----------------------------------------------------------------------------
# a contributor without bot in his name is considered a bot if his number
# of contributions is greater than or equal to HUMAN_BOT_THRESHOLD
HUMAN_BOT_THRESHOLD = 8000
# SCORE_DENOMINATOR_POWER is used to compute the similarity score
# between two articles. It indicates the power of the normalization of the
# score.
# E.g. 0.55 will yield the formula co_con(A,B)/(con(A)+con(B))^0.55
# which is a little more than the square root
SCORE_DENOMINATOR_POWER = 0.6
# -----------------------------------------------------------------------------


# co_authorship generation
# -----------------------------------------------------------------------------
# If COAUTH_UPDATE is set to True, the count gets updated, otherwise
# duplicates are ignored. Setting this to False increases speed
COAUTH_UPDATE = True
# only pairs with IDs below COAUTH_ID_LIMIT will be generated
COAUTH_LIMIT = 10000
# size of ID intervals (after the intial interval)
COAUTH_INTERVAL_SIZE = 200
# Size of the first block can be bigger as this speeds up the generation.
# Only relevant, when COAUTH_INITIAL_OFFSET = 0
COAUTH_INITIAL_INTERVAL_SIZE = 1000
# Lower ID-Limit at which to start. All pairs between articles with lower
# IDs must be already present. This option is useful for iterativly
# expanding the co-authorship dataset or when the execution of the script
# was interrupted
COAUTH_INITIAL_OFFSET = 0
# Sets how the number of edits (or articles in th case of categories) in a
# contribution counts. The number is raised by this power.
COAUTH_COUNT_POWER = 0.6
# Sets how the time between contributions counts. The DATEDIFF is raised
# by this power.
COAUTH_TIME_POWER = 0.3
# -----------------------------------------------------------------------------


# exporting the BEST_EXPORT_NUM_EDGES best co-authorship connections (highest
# similarity score/weight) and the articles/categories contained in them
# -----------------------------------------------------------------------------
# how many co-authorship edges (similarities) should be exported
BEST_EXPORT_NUM_EDGES = 5000
# node and edge CSV files
BEST_EXPORT_NODE_FILE = 'wdir/best_node.csv'
BEST_EXPORT_EDGE_FILE = 'wdir/best_edge.csv'
# GraphML file
BEST_EXPORT_GRAPH_FILE = 'wdir/output.graphml'
# -----------------------------------------------------------------------------
