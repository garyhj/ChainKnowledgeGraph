#!/usr/bin/env python3
# coding: utf-8
# File: MedicalGraph.py
# Author: lhy<lhy_in_blcu@126.com,https://liuhuangyong.github.io>
# Date: 18-10-3
# Optimized: 2025-12-19 for performance

import os
import json
from py2neo import Graph

class MedicalGraph:
    def __init__(self):
        cur_dir = os.path.dirname(os.path.abspath(__file__))
        self.data_dir = os.path.join(cur_dir, 'data')
        self.g = Graph(
            host="127.0.0.1",
            http_port=7474,
            user="neo4j",
            password="123456"
        )

    def load_data(self, filename):
        path = os.path.join(self.data_dir, filename)
        datas = []
        with open(path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        obj = json.loads(line)
                        if obj:
                            datas.append(obj)
                    except json.JSONDecodeError:
                        continue
        return datas

    def create_nodes_batch(self, label: str, nodes: list, name_key: str = "name"):
        """
        批量创建节点，使用 UNWIND 提升性能
        假设每个 node 是 dict，包含属性，其中必须有 name 字段（或指定 name_key）
        """
        if not nodes:
            return
        # 构造属性字典列表（确保所有字段都传入）
        batch_data = []
        for node in nodes:
            # 过滤掉 None 或空字符串值（可选）
            cleaned = {k: v for k, v in node.items() if v is not None and v != ""}
            batch_data.append(cleaned)

        query = f"""
        UNWIND $batch AS row
        CREATE (n:{label})
        SET n = row
        """
        self.g.run(query, batch=batch_data)
        print(f"Created {len(batch_data)} nodes of type '{label}'")

    def create_relationships_batch(self, start_label, end_label, rel_type, edges, from_key, to_key, attr_keys=None):
        """
        批量创建关系
        :param attr_keys: 需要作为关系属性的字段名列表，如 ["rel_weight"]
        """
        if not edges:
            return

        batch_data = []
        for edge in edges:
            item = {
                "from": edge[from_key],
                "to": edge[to_key],
            }
            if attr_keys:
                for k in attr_keys:
                    if k in edge:
                        item[k] = edge[k]
            batch_data.append(item)

        # 构建 SET 子句（如果有属性）
        set_clause = ""
        if attr_keys:
            set_items = [f"rel.{k} = row.{k}" for k in attr_keys]
            set_clause = " SET " + ", ".join(set_items)

        query = f"""
        UNWIND $batch AS row
        MATCH (a:{start_label} {{name: row.from}})
        MATCH (b:{end_label} {{name: row.to}})
        CREATE (a)-[rel:{rel_type}]->(b)
        {set_clause}
        """
        self.g.run(query, batch=batch_data)
        print(f"Created {len(batch_data)} relationships of type '{rel_type}'")

    def create_graphnodes(self):
        company = self.load_data('company.json')
        product = self.load_data('product.json')
        industry = self.load_data('industry.json')

        self.create_nodes_batch('company', company)
        self.create_nodes_batch('product', product)
        self.create_nodes_batch('industry', industry)

    def create_graphrels(self):
        company_industry = self.load_data('company_industry.json')
        company_product = self.load_data('company_product.json')
        product_product = self.load_data('product_product.json')
        industry_industry = self.load_data('industry_industry.json')

        # 注意：假设所有关系数据中都有 "rel" 字段表示关系类型
        # 但不同文件可能结构不同，需统一处理

        # 1. company -[rel]-> industry
        self.create_relationships_batch('company', 'industry', 'BELONGS_TO', company_industry, 'company_name', 'industry_name')

        # 2. industry -> industry （假设 rel 字段存在）
        # 如果 industry_industry 中的关系类型固定，可硬编码；否则需动态处理
        # 此处假设关系类型在 "rel" 字段中
        if industry_industry and 'rel' in industry_industry[0]:
            # 动态按 rel 分组（更复杂），但为简化，先假设都是同一类型如 "SUBCLASS_OF"
            # 或者你可以在 JSON 中统一用固定 rel 名
            self.create_relationships_batch('industry', 'industry', 'RELATED_TO', industry_industry, 'from_industry', 'to_industry')

        # 3. company -[PRODUCES {权重: ...}]-> product
        self.create_relationships_batch('company', 'product', 'PRODUCES', company_product, 'company_name', 'product_name', attr_keys=['rel_weight'])

        # 4. product -[SIMILAR_TO]-> product
        self.create_relationships_batch('product', 'product', 'SIMILAR_TO', product_product, 'from_entity', 'to_entity')

if __name__ == '__main__':
    handler = MedicalGraph()
    handler.create_graphnodes()
    handler.create_graphrels()
