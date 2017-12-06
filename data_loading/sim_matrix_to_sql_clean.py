import sqlite3
import csv
import pandas as pd

def sim_matrix_to_sql_table(filename):
    con_wdb = sqlite3.connect('wineapp.db')

    c = con_wdb.cursor()

    #################################################################################
    # THIS ABSOLUTELY HAS TO MATCH cleanReviews.py SQL_Query for Grabbing ID's
    # OTHERWISE USELESS
    # Think about reading a .csv generated from that routine
    #################################################################################

    sql = '''SELECT wine_wineId_int FROM wines ORDER by wine_qty_reviews DESC LIMIT 50'''
    wine_ids = pd.read_sql(sql, con_wdb)

    #################################################################################
    #################################################################################
    #################################################################################

    c.execute('DROP TABLE IF EXISTS similar_wines')
    c.execute('''
                CREATE TABLE similar_wines (
                similar_wines_wineId,
                sim_wineProxy_id_garbage,
                wineId_match_1,
                wineId_match_2,
                wineId_match_3,
                wineId_match_4,
                wineId_match_5,
                wineId_match_6,
                wineId_match_7,
                wineId_match_8,
                wineId_match_9,
                wineId_match_10);''')
    con_wdb.commit()

    # Create a CSV reader
    reader = csv.reader(open(filename))

    # create index and move to next row and store old index
    wine_index = next(reader, None)[1:]
    wine_index_int = [int(x) for x in wine_index]

    # How many similar items would you like to store?
    similar_items = 10  # RIGHT NOW THIS DOESN'T WORK. MUST UPDATE QUERY, TOO.

    for main_wine_index, row in enumerate(reader):
        try:
            row = [float(x) for x in row]
            stacked = list(zip(row[1:], wine_ids['wine_wineId_int'].values, wine_index_int))
            stacked.sort(key=lambda x: x[0], reverse=True)

            write_out_list = ['']*(similar_items+2)
            write_out_list[1] = int(stacked[0][2])
            write_out_list[0] = int(stacked[0][1])

            index_no = 1
            for most_similar in range(similar_items):
                write_out_list[index_no+1] = int(stacked[index_no][1])
                index_no += 1

            c.execute('''
                INSERT INTO similar_wines (
                similar_wines_wineId,
                sim_wineProxy_id_garbage,
                wineId_match_1,
                wineId_match_2,
                wineId_match_3,
                wineId_match_4,
                wineId_match_5,
                wineId_match_6,
                wineId_match_7,
                wineId_match_8,
                wineId_match_9,
                wineId_match_10)
                 VALUES (?,?,?,?,?,?,?,?,?,?,?,?)''',
                     (write_out_list[0],
                      write_out_list[1],
                      write_out_list[2],
                      write_out_list[3],
                      write_out_list[4],
                      write_out_list[5],
                      write_out_list[6],
                      write_out_list[7],
                      write_out_list[8],
                      write_out_list[9],
                      write_out_list[10],
                      write_out_list[11]))
        # Catch errors
        except sqlite3.Error as e:
            print("Sim Matrix to SQL, insert error:", e.args[0])
    con_wdb.commit()


if __name__ == '__main__':

    sim_matrix_to_sql_table('similarity_matrix.csv')




