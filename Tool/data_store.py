import logging
import time
import pymongo
import csv
from collections import deque


class CsvToMongoDB:
    """
    CsvToMongoDB类用于处理标准 SQL 型的 CSV 文件，并将内容导入至 MongoDB。

    Parameters:
        - db_name (str): MongoDB 数据库名称。
        - csv_path (dict): 包含集合名与 csv 文件路径映射的字典，如 {"集合名1": "CSV文件路径1", "集合名2": "CSV文件路径2"}。
        - db_url (str, optional): MongoDB 连接地址，默认为"localhost"。
        - port (int, optional): MongoDB 连接端口号，默认为27017。
        - db_check (bool, optional): 是否在插入数据前清空数据库中对应的集合，默认为 False。
        - auto_insert (bool, optional): 是否在初始化时自动将 csv 数据插入数据库。启用将导致无法进行其他参数设置，默认为 False。

    Methods:
        - assure_empty(): 清空数据库中指定的集合，如果 db_check 设置为 True，则在自动插入数据前调用该方法。
        - set_target_col(col_name: str): 设置当前操作的目标集合。
        - set_csv_property(newline='\n', encoding='utf-8', delimiter=','): 设置 csv 文件的读取属性。
        - get_csv_property(): 获取当前设置的 csv 文件读取属性。
        - get_origin_fields(): 获取源 csv 文件中的所有字段名。
        - set_fields(fields_needed=None, fields_removed=None): 设置需要保留的或者删除的字段。
        - insert_data(col_name: str, buffer_size=10000): 将指定 csv 文件中的数据插入到对应的集合中。
        - auto_insert(): 自动将所有给定的 csv 文件转换为 MongoDB 集合。
    """

    def __init__(self, db_name: str, csv_path: dict, db_url: str = "localhost", port: int = 27017, db_check=False,
                 auto_insert=False):
        self.target_col_name = ""
        self.target_col_object = None  # 当前操作集合对象
        self.csv_property = {"newline": '\n', "encoding": 'utf-8', "delimiter": ','}
        self.db = pymongo.MongoClient(db_url, port)[db_name]
        # self.collections 示例：{col_name:(col_object, csv_path)}
        self.collections = {}
        for col_name in csv_path.keys():
            self.collections[col_name] = (self.db[col_name], csv_path[col_name])
        self.db_check = db_check
        self.selected_fields = []
        self.removed_fields = []
        if auto_insert:
            self.auto_insert()

    def assure_empty(self, col_name="") -> None:
        """清空数据库中指定的集合。如果db_check设置为True，则在自动插入数据前调用该方法。"""
        if col_name:
            self.collections[col_name][0].drop()
        else:
            self.target_col_object.drop()

    def set_target_col(self, col_name: str) -> None:
        """
        设置当前操作的目标集合。
        :param  col_name: 目标集合的名称。
        :return: None
        """
        self.target_col_name = col_name
        self.target_col_object = self.collections[col_name][0]

    def set_csv_property(self, newline='\n', encoding='utf-8', delimiter=',') -> None:
        """
        设置CSV文件的读取属性
        :param newline: CSV文件中行之间的分隔符，默认为 "\\n"
        :param encoding: CSV文件的编码方式，默认为 "utf-8"
        :param delimiter: CSV文件中字段之间的分隔符，默认为 ","
        :return: None
        """
        self.csv_property = {"newline": newline, "encoding": encoding, "delimiter": delimiter}

    def get_csv_property(self):
        """
        获取当前设置的CSV文件读取属性。
        :return: dict; 包含CSV文件读取属性的字典，格式为{"newline": '\n', "encoding": 'utf-8', "delimiter": ','}。
        """
        return self.csv_property

    def get_origin_fields(self, col_name) -> list[str]:
        """
        获取源 csv 文件中的所有字段名。
        :param col_name: 集合名称
        :return: 包含所有字段名称的列表
        """
        with open(self.collections[col_name][1], 'r', newline=self.csv_property["newline"],
                  encoding=self.csv_property["encoding"]) as csvfile:
            db_reader = csv.reader(csvfile, delimiter=self.csv_property["delimiter"])
            return next(db_reader)

    def set_fields(self, fields_needed: list[str] = None, fields_removed: list[str] = None) -> None:
        """
        设置需要保留的或者删除的字段。
        :param fields_needed: 需要保留的字段，形式为[field_name1, field_name2]
        :param fields_removed: 需要删除的字段，形式为[field_name1, field_name2]
        :return: None
        """
        if fields_needed and fields_removed:
            logging.warning("参数错误！需要保留的字段和需要删除的字段不能同时给出！")
            raise SyntaxError("需要保留的字段和需要删除的字段不能同时给出！")
        if not (fields_needed or fields_removed):
            return
        if fields_needed:
            self.selected_fields = fields_needed
        elif fields_removed:
            self.removed_fields = fields_removed

    def insert_data(self, col_name: str, buffer_size=10000) -> None:
        """
        将指定CSV文件中的数据插入到对应的集合中。
        :param col_name: 需要插入数据的集合名称。
        :param buffer_size: 数据插入时的缓冲区大小，默认为10000
        :return: None
        """
        self.set_target_col(col_name)
        if self.target_col_object is None:
            logging.warning("未设置需要操作的集合对象")
            raise ValueError("未设置需要操作的集合对象")
        # TODO 转换进度条
        # 为防止占用过多内存，使用缓冲区读写数据
        if self.db_check:
            self.assure_empty()
        logging.info(f"集合 {self.target_col_name} 开始转换")
        start = time.time()
        with open(self.collections[col_name][1], 'r', newline=self.csv_property["newline"],
                  encoding=self.csv_property["encoding"]) as csvfile:
            db_reader = csv.reader(csvfile, delimiter=self.csv_property["delimiter"])
            header = next(db_reader)
            field_allowed = list(range(len(header)))
            try:
                if self.selected_fields:
                    field_allowed = []
                    for field in self.selected_fields:
                        field_allowed.append(header.index(field))
                elif self.removed_fields:
                    field_allowed = [i for i in field_allowed if i not in [header.index(field) for field in self.removed_fields]]
            except ValueError as e:
                logging.warning(e)
                raise ValueError("指定的字段不存在")
            header = [header[i] for i in field_allowed]
            logging.info(f"保留的字段为{header}")
            buffer = deque()
            counter = 0
            for row in db_reader:
                counter += 1
                drug = dict(zip(header, [row[i] for i in field_allowed]))
                buffer.append(drug)
                if len(buffer) >= buffer_size:
                    requests = [pymongo.InsertOne(doc) for doc in buffer]
                    self.target_col_object.bulk_write(requests, ordered=False)
                    buffer.clear()
            # 确保缓冲区中的数据被全部取出
            if buffer:
                requests = [pymongo.InsertOne(doc) for doc in buffer]
                self.target_col_object.bulk_write(requests, ordered=False)
            # return time.time()-start
        logging.info(f"集合 {self.target_col_name} 转换完成，共插入 {counter} 条文档，用时{time.time() - start:.2f}s")

    def auto_insert(self):
        """
        自动将所有给定的 csv 文件转换为 MongoDB 集合。
        :return: None
        """
        # TODO 多线程优化
        # 但可能主要耗时在IO
        for col_name in self.collections.keys():
            self.insert_data(col_name)
        logging.info("所有csv文件均处理完毕")


if __name__ == "__main__":
    # 以下为用法示例
    logging.basicConfig(filename="../log/db_log.txt", level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s',
                        datefmt='%m/%d %I:%M:%S')
    csv_path = {
        "offsides": r"C:\Users\deepwind\Desktop\drug database\data\OFFSIDES.csv",
        "twosides": r"C:\Users\deepwind\Desktop\drug database\data\TWOSIDES.csv"
    }
    drugDB = CsvToMongoDB("drugdb", csv_path, db_check=True, auto_insert=True)
    # drugDB.insert_data("offsides")
    # drugDB.insert_data("twosides")
