# Folder setup
temp_folder = "../../temp/indexing/"
output_folder = "../../output/indexing/"
index_folder = "../../index/"
misc_folder = "../../index/"

stopwords_file = "../../resources/stopwords.txt"


temp_index_file = "../../temp/indexing/temp.index"
index_file = index_folder + "{}.index"
catalog_file = misc_folder + "{}.catalog"
term_file = misc_folder + "{}.termmap"
doc_file = misc_folder + "{}.docmap"
dlen = misc_folder + "{}.dlen"

# Crawler Constants
seed_url = "http://en.wikipedia.org/wiki/Three_Mile_Island_accident"
DEFAULT_INDEX_PAGES = ["index.html", "index.htm", "home.html", "home.htm"]
FILE_TYPES = [".html", ".htm", ".json"]