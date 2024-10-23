# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
from datetime import date

import pymysql
# useful for handling different item types with a single interface
from itemadapter import ItemAdapter

from zepto_.items import Zepto_roshi, Zepto_comp


class ZeptoPipeline:
    today_date = str(date.today()).replace('-', '_')

    def __init__(self):
        try:
            self.conn = pymysql.Connect(host='localhost',
                                        user='root',
                                        password='actowiz',
                                        database='zepto_')
            self.cur = self.conn.cursor()

        except Exception as e:
            print(e)

    def process_item(self, item, spider):

        if isinstance(item, Zepto_roshi):
            try:
                self.cur.execute(
                    f"CREATE TABLE IF NOT EXISTS Zepto_roshi_data_table_{self.today_date}(id INT AUTO_INCREMENT PRIMARY KEY,unique_id varchar(255) unique)")
                self.cur.execute(f"SHOW COLUMNS FROM Zepto_roshi_data_table_{self.today_date}")
                existing_columns = [column[0] for column in self.cur.fetchall()]
                item_columns = [column_name.replace(" ", "_") if " " in column_name else column_name for column_name in
                                item.keys()]
                for column_name in item_columns:
                    if column_name not in existing_columns:
                        column_name = column_name.lower()
                        try:
                            self.cur.execute(
                                f"ALTER TABLE Zepto_roshi_data_table_{self.today_date} ADD COLUMN `{column_name}` LONGTEXT")
                            existing_columns.append(column_name)
                        except Exception as e:
                            print(e)
            except Exception as e:
                print(e)

            try:
                field_list = []
                value_list = []
                for field in item:
                    field_list.append(str(field))
                    value_list.append('%s')
                fields = ','.join(field_list)
                values = ", ".join(value_list)
                insert_query = f"insert ignore into Zepto_roshi_data_table_{self.today_date}( " + fields + " ) values ( " + values + " )"
                self.cur.execute(insert_query, tuple(item.values()))
                self.conn.commit()
            except Exception as e:
                print(e)

            try:
                # Update `master_table` status
                if 'unique_id' in item:
                    update_query = "UPDATE zepto_links_roshi SET status = 'Done' WHERE unique_id = %s"
                    self.cur.execute(update_query, (item['unique_id'],))
                    self.conn.commit()
                else:
                    print("unique_id not found in item.")
            except Exception as e:
                print(f"Error updating master_table: {e}")



        if isinstance(item, Zepto_comp):
            try:
                self.cur.execute(
                    f"CREATE TABLE IF NOT EXISTS Zepto_comp_data_table_{self.today_date}(id INT AUTO_INCREMENT PRIMARY KEY,unique_id varchar(255) unique)")
                self.cur.execute(f"SHOW COLUMNS FROM Zepto_comp_data_table_{self.today_date}")
                existing_columns = [column[0] for column in self.cur.fetchall()]
                item_columns = [column_name.replace(" ", "_") if " " in column_name else column_name for column_name in
                                item.keys()]
                for column_name in item_columns:
                    if column_name not in existing_columns:
                        column_name = column_name.lower()
                        try:
                            self.cur.execute(
                                f"ALTER TABLE Zepto_comp_data_table_{self.today_date} ADD COLUMN `{column_name}` LONGTEXT")
                            existing_columns.append(column_name)
                        except Exception as e:
                            print(e)
            except Exception as e:
                print(e)

            try:
                field_list = []
                value_list = []
                for field in item:
                    field_list.append(str(field))
                    value_list.append('%s')
                fields = ','.join(field_list)
                values = ", ".join(value_list)
                insert_query = f"insert ignore into Zepto_comp_data_table_{self.today_date}( " + fields + " ) values ( " + values + " )"
                self.cur.execute(insert_query, tuple(item.values()))
                self.conn.commit()
            except Exception as e:
                print(e)

            try:
                # Update `master_table` status
                if 'unique_id' in item:
                    update_query = "UPDATE zepto_links_comp SET status = 'Done' WHERE unique_id = %s"
                    self.cur.execute(update_query, (item['unique_id'],))
                    self.conn.commit()
                else:
                    print("unique_id not found in item.")
            except Exception as e:
                print(f"Error updating master_table: {e}")

        return item
