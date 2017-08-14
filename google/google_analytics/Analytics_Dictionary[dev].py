from . import pg_connector

db = pg_connector.dbinfo()

delimiters = ['.', ',', '\'\'', ';', ':', '?', '!', '@', '(', ')', '>', '<', '/', '\\', '-', '`', ']', '[']
variants = ['[*]', '[ *]', '[* ]', '[Jk *]', '[Jw *]', '[* cak]', '[* Fis]', '[* Dok]', '[* Mil]']
adjectiva = list()
adverb = list()
verba = list()
nomina = list()
particle = list()
for variant in variants:
    adjectiva.append(variant.replace('*', 'a'))
    adverb.append(variant.replace('*', 'adv'))
    verba.append(variant.replace('*', 'v'))
    nomina.append(variant.replace('*', 'n'))
    particle.append(variant.replace('*', 'p'))


def init_table():
    query = "create table if not exists AnalyticsDict(word varchar(100), category varchar(50), primary key(word)); "
    query += "create table if not exists stopwords(word varchar(50), primary key(word));"
    pg_connector.executeQuery(db, query)


# Add word to target_table
def addWord(source_table, column_name, dictionary_table='AnalyticsDict', stopwords_table='stopwords', temporary_table='temp_dict'):
    query = "select distinct lower(word) as word, category into " + temporary_table + \
            " from (select Cast(unnest(string_to_array(" + column_name +\
            ", ' ')) as varchar(100)) as word, cast('' as varchar(50)) as category " +\
            "from " + source_table + ") a"
    pg_connector.executeQuery(db, query)

    #eliminate symbols
    for delimiter in delimiters:
        query = "update " + temporary_table + " set word=replace(word,'" + delimiter + "', ''); "
        pg_connector.executeQuery(db, query)

    #eliminate stop word
    query = "delete from " + temporary_table + " where word in(select word from " + stopwords_table + ");"
    pg_connector.executeQuery(db, query)

    #delete to avoid duplicate value
    query = "delete from " + temporary_table + " a where exists(select 1 from " + source_table + " b where b.word=a.word);"
    pg_connector.executeQuery(db, query)

    #delete word with length < 3
    query = "delete from " + temporary_table + " where length(word)<=2"
    pg_connector.executeQuery(db, query)

    #delete number
    query = "delete from " + temporary_table + " where word similar to '\d*'"
    pg_connector.executeQuery(db, query)

    #insert into AnalyticsDict
    query = "insert into " + dictionary_table + " select distinct * from " + temporary_table + "; drop table " + temporary_table + ";"
    pg_connector.executeQuery(db, query)

    classifiedWords(dictionary_table, stopwords_table)



def classifiedWords(dictionary_table, stopwords_table = 'stopwords'):
    #based on http://kbbi.web.id
    import requests, lxml.html, re
    from lxml.cssselect import CSSSelector


    sel = CSSSelector('td.tr2')
    while(pg_connector.getData(db, "select 1 from " + dictionary_table + " where coalesce(category,'')='' limit 1")[0][0] == 1):
        #get 1000 word only
        words = pg_connector.getData(db, "select word from " + dictionary_table +
                                         " where coalesce(category,'')='' limit 100")
        for word in words:
            r = requests.get('http://kamusbahasaindonesia.org/' + word[0])
            tree = lxml.html.fromstring(r.text)
            try:
                content = sel(tree)[0].text_content()
                category = re.search(r"\[.{1,7}\]", content).group(0)
            except:
                pg_connector.executeQuery(db, "update " + dictionary_table + " set category='(undefined)' where word='" + word[0] + "';")
                continue
            if category in particle:
                print("insert " + word[0] + " as stop word")
                pg_connector.executeQuery(db, "insert into " + stopwords_table + " values('" + word[0] + "');")
                pg_connector.executeQuery(db, "delete from " + dictionary_table + " where word='" + word[0] + "'")

            elif category in nomina:
                pg_connector.executeQuery(db, "update " + dictionary_table + " set category='nomina' where word='" + word[0] + "';")
            elif category in adjectiva:
                pg_connector.executeQuery(db, "update " + dictionary_table + " set category='adjektiva' where word='" + word[0] + "';")
            elif category in verba:
                pg_connector.executeQuery(db, "update " + dictionary_table + " set category='verba' where word='" + word[0] + "';")
            elif category in adverb or category == '[Bld]':
                pg_connector.executeQuery(db, "update " + dictionary_table + " set category='adverb' where word='" + word[0] + "';")
            else:
                print(word[0] + " - " + category)
                exit()

