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

    def ensure_index_and_constraint(self, label: str):
        """为 label 的 name 属性创建唯一性约束（自动包含索引）"""
        try:
            # 先尝试创建唯一性约束（Neo4j 5+ 推荐方式）
            self.g.run(f"CREATE CONSTRAINT IF NOT EXISTS FOR (n:{label}) REQUIRE n.name IS UNIQUE")
            print(f"✅ Constraint (and index) created for {label}.name")
        except Exception as e:
            # 如果不支持约束（如旧版），退化为创建索引
            print(f"Constraint failed for {label}, falling back to index: {e}")
            try:
                self.g.run(f"CREATE INDEX IF NOT EXISTS FOR (n:{label}) ON (n.name)")
                print(f"✅ Index created for {label}.name")
            except Exception as e2:
                print(f"Index also failed for {label}: {e2}")


    def create_indexes_and_constraints(self):
        print("🔍 Creating indexes and constraints...")
        self.ensure_index_and_constraint('company')
        self.ensure_index_and_constraint('product')
        self.ensure_index_and_constraint('industry')

    
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

    def create_graphnodes(self):
        company = self.load_data('company.json')
        product = self.load_data('product.json')
        industry = self.load_data('industry.json')
        self.create_nodes_batch('company', company)
        self.create_nodes_batch('product', product)
        self.create_nodes_batch('industry', industry)


    def create_relationships_dynamic(self, start_label, end_label, edges, from_key, to_key, attr_keys=None):
        """
        动态按 edge['rel'] 分组，批量创建不同关系类型
        """
        if not edges:
            return

        rel_groups = defaultdict(list)
        for edge in edges:
            rel_type = edge.get("rel")
            if not rel_type or from_key not in edge or to_key not in edge:
                continue
            item = {
                "from": str(edge[from_key]),
                "to": str(edge[to_key]),
            }
            if attr_keys:
                for k in attr_keys:
                    if k in edge:
                        item[k] = edge[k]
            rel_groups[rel_type].append(item)

        for rel_type, batch in rel_groups.items():
            # 构建关系属性 SET 子句
            set_clause = ""
            if attr_keys:
                existing_attrs = set()
                for item in batch:
                    existing_attrs.update(k for k in attr_keys if k in item)
                if existing_attrs:
                    set_items = [f"rel.{k} = row.{k}" for k in existing_attrs]
                    set_clause = " SET " + ", ".join(set_items)

            # 注意：关系类型用反引号包裹，支持含空格/特殊字符
            query = f"""
            UNWIND $batch AS row
            MATCH (a:{start_label} {{name: row.from}})
            MATCH (b:{end_label} {{name: row.to}})
            CREATE (a)-[rel:`{rel_type}`]->(b)
            {set_clause}
            """
            try:
                self.g.run(query, batch=batch)
                print(f"✅ Created {len(batch)} relationships of type `{rel_type}` "
                      f"from {start_label} to {end_label}")
            except Exception as e:
                print(f"❌ Failed to create relationship `{rel_type}`: {e}")

    def create_graphrels(self):
        print("🔗 Loading relationship data...")
        company_industry = self.load_data('company_industry.json')
        company_product = self.load_data('company_product.json')
        product_product = self.load_data('product_product.json')
        industry_industry = self.load_data('industry_industry.json')
        print("⚡ Creating relationships...")
        self.create_relationships_dynamic('company', 'industry', company_industry, 'company_name', 'industry_name')
        self.create_relationships_dynamic('industry', 'industry', industry_industry, 'from_industry', 'to_industry')
        self.create_relationships_dynamic('company', 'product', company_product, 'company_name', 'product_name', attr_keys=['rel_weight'])
        self.create_relationships_dynamic('product', 'product', product_product, 'from_entity', 'to_entity')

if __name__ == '__main__':
    handler = MedicalGraph()
    handler.create_graphnodes()
    handler.create_indexes_and_constraints()  
    handler.create_graphrels()
