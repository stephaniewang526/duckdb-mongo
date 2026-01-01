#!/usr/bin/env python3
"""
MongoDB aggregation queries for TPC-H benchmark comparison.
These are equivalent to DuckDB's PRAGMA tpch() queries.
"""

from pymongo import MongoClient
from datetime import datetime
import sys


def get_mongodb_queries():
    """Return dictionary of TPC-H query aggregation pipelines."""
    
    queries = {}
    
    # Q1: Pricing Summary Report
    queries[1] = [
        {
            "$match": {
                "l_shipdate": {"$lte": datetime(1998, 9, 2)}
            }
        },
        {
            "$group": {
                "_id": {
                    "returnflag": "$l_returnflag",
                    "linestatus": "$l_linestatus"
                },
                "sum_qty": {"$sum": "$l_quantity"},
                "sum_base_price": {"$sum": {"$multiply": ["$l_quantity", "$l_extendedprice"]}},
                "sum_disc_price": {
                    "$sum": {
                        "$multiply": [
                            {"$multiply": ["$l_quantity", "$l_extendedprice"]},
                            {"$subtract": [1, "$l_discount"]}
                        ]
                    }
                },
                "sum_charge": {
                    "$sum": {
                        "$multiply": [
                            {"$multiply": ["$l_quantity", "$l_extendedprice"]},
                            {"$subtract": [1, "$l_discount"]},
                            {"$add": [1, "$l_tax"]}
                        ]
                    }
                },
                "avg_qty": {"$avg": "$l_quantity"},
                "avg_price": {"$avg": "$l_extendedprice"},
                "avg_disc": {"$avg": "$l_discount"},
                "count_order": {"$sum": 1}
            }
        },
        {
            "$sort": {
                "_id.returnflag": 1,
                "_id.linestatus": 1
            }
        },
        {
            "$project": {
                "_id": 0,
                "l_returnflag": "$_id.returnflag",
                "l_linestatus": "$_id.linestatus",
                "sum_qty": 1,
                "sum_base_price": 1,
                "sum_disc_price": 1,
                "sum_charge": 1,
                "avg_qty": 1,
                "avg_price": 1,
                "avg_disc": 1,
                "count_order": 1
            }
        }
    ]
    
    # Q2: Minimum Cost Supplier Query
    queries[2] = [
        {
            "$match": {
                "r_name": "EUROPE"
            }
        },
        {
            "$lookup": {
                "from": "nation",
                "localField": "r_regionkey",
                "foreignField": "n_regionkey",
                "as": "nations"
            }
        },
        {"$unwind": "$nations"},
        {
            "$lookup": {
                "from": "supplier",
                "localField": "nations.n_nationkey",
                "foreignField": "s_nationkey",
                "as": "suppliers"
            }
        },
        {"$unwind": "$suppliers"},
        {
            "$lookup": {
                "from": "partsupp",
                "localField": "suppliers.s_suppkey",
                "foreignField": "ps_suppkey",
                "as": "partsupps"
            }
        },
        {"$unwind": "$partsupps"},
        {
            "$lookup": {
                "from": "part",
                "localField": "partsupps.ps_partkey",
                "foreignField": "p_partkey",
                "as": "parts"
            }
        },
        {"$unwind": "$parts"},
        {
            "$match": {
                "parts.p_size": 15,
                "parts.p_type": {"$regex": ".*BRASS$"}
            }
        },
        {
            "$group": {
                "_id": "$partsupps.ps_partkey",
                "min_supplycost": {"$min": "$partsupps.ps_supplycost"}
            }
        },
        {
            "$lookup": {
                "from": "partsupp",
                "let": {"partkey": "$_id", "mincost": "$min_supplycost"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$ps_partkey", "$$partkey"]},
                                    {"$eq": ["$ps_supplycost", "$$mincost"]}
                                ]
                            }
                        }
                    },
                    {
                        "$lookup": {
                            "from": "supplier",
                            "localField": "ps_suppkey",
                            "foreignField": "s_suppkey",
                            "as": "supplier_info"
                        }
                    },
                    {"$unwind": "$supplier_info"},
                    {
                        "$lookup": {
                            "from": "nation",
                            "localField": "supplier_info.s_nationkey",
                            "foreignField": "n_nationkey",
                            "as": "nation_info"
                        }
                    },
                    {"$unwind": "$nation_info"},
                    {
                        "$lookup": {
                            "from": "region",
                            "localField": "nation_info.n_regionkey",
                            "foreignField": "r_regionkey",
                            "as": "region_info"
                        }
                    },
                    {"$unwind": "$region_info"},
                    {
                        "$match": {
                            "region_info.r_name": "EUROPE"
                        }
                    },
                    {
                        "$lookup": {
                            "from": "part",
                            "localField": "ps_partkey",
                            "foreignField": "p_partkey",
                            "as": "part_info"
                        }
                    },
                    {"$unwind": "$part_info"}
                ],
                "as": "matches"
            }
        },
        {"$unwind": "$matches"},
        {
            "$project": {
                "_id": 0,
                "s_acctbal": "$matches.supplier_info.s_acctbal",
                "s_name": "$matches.supplier_info.s_name",
                "n_name": "$matches.nation_info.n_name",
                "p_partkey": "$matches.part_info.p_partkey",
                "p_mfgr": "$matches.part_info.p_mfgr",
                "s_address": "$matches.supplier_info.s_address",
                "s_phone": "$matches.supplier_info.s_phone",
                "s_comment": "$matches.supplier_info.s_comment"
            }
        },
        {
            "$sort": {
                "s_acctbal": -1,
                "n_name": 1,
                "s_name": 1,
                "p_partkey": 1
            }
        },
        {"$limit": 100}
    ]
    
    # Q3: Shipping Priority Query
    queries[3] = [
        {
            "$match": {
                "c_mktsegment": "BUILDING"
            }
        },
        {
            "$lookup": {
                "from": "orders",
                "localField": "c_custkey",
                "foreignField": "o_custkey",
                "as": "orders"
            }
        },
        {"$unwind": "$orders"},
        {
            "$match": {
                "orders.o_orderdate": {"$lt": datetime(1995, 3, 15)}
            }
        },
        {
            "$lookup": {
                "from": "lineitem",
                "localField": "orders.o_orderkey",
                "foreignField": "l_orderkey",
                "as": "lineitems"
            }
        },
        {"$unwind": "$lineitems"},
        {
            "$match": {
                "lineitems.l_shipdate": {"$gt": datetime(1995, 3, 15)}
            }
        },
        {
            "$group": {
                "_id": {
                    "orderkey": "$orders.o_orderkey",
                    "orderdate": "$orders.o_orderdate",
                    "shippriority": "$orders.o_shippriority"
                },
                "revenue": {
                    "$sum": {
                        "$multiply": [
                            "$lineitems.l_extendedprice",
                            {"$subtract": [1, "$lineitems.l_discount"]}
                        ]
                    }
                }
            }
        },
        {
            "$project": {
                "_id": 0,
                "l_orderkey": "$_id.orderkey",
                "revenue": 1,
                "o_orderdate": "$_id.orderdate",
                "o_shippriority": "$_id.shippriority"
            }
        },
        {
            "$sort": {
                "revenue": -1,
                "o_orderdate": 1
            }
        },
        {"$limit": 10}
    ]
    
    # Q4: Order Priority Checking Query
    queries[4] = [
        {
            "$match": {
                "o_orderdate": {
                    "$gte": datetime(1993, 7, 1),
                    "$lt": datetime(1993, 10, 1)
                }
            }
        },
        {
            "$lookup": {
                "from": "lineitem",
                "localField": "o_orderkey",
                "foreignField": "l_orderkey",
                "as": "lineitems",
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$lt": ["$l_commitdate", "$l_receiptdate"]
                            }
                        }
                    }
                ]
            }
        },
        {
            "$match": {
                "lineitems": {"$ne": []}
            }
        },
        {
            "$group": {
                "_id": "$o_orderpriority",
                "order_count": {"$sum": 1}
            }
        },
        {
            "$project": {
                "_id": 0,
                "o_orderpriority": "$_id",
                "order_count": 1
            }
        },
        {
            "$sort": {
                "o_orderpriority": 1
            }
        }
    ]
    
    # Q5: Local Supplier Volume Query
    queries[5] = [
        {
            "$match": {
                "r_name": "ASIA"
            }
        },
        {
            "$lookup": {
                "from": "nation",
                "localField": "r_regionkey",
                "foreignField": "n_regionkey",
                "as": "nations"
            }
        },
        {"$unwind": "$nations"},
        {
            "$lookup": {
                "from": "customer",
                "localField": "nations.n_nationkey",
                "foreignField": "c_nationkey",
                "as": "customers"
            }
        },
        {"$unwind": "$customers"},
        {
            "$lookup": {
                "from": "orders",
                "localField": "customers.c_custkey",
                "foreignField": "o_custkey",
                "as": "orders"
            }
        },
        {"$unwind": "$orders"},
        {
            "$match": {
                "orders.o_orderdate": {
                    "$gte": datetime(1994, 1, 1),
                    "$lt": datetime(1995, 1, 1)
                }
            }
        },
        {
            "$lookup": {
                "from": "lineitem",
                "localField": "orders.o_orderkey",
                "foreignField": "l_orderkey",
                "as": "lineitems"
            }
        },
        {"$unwind": "$lineitems"},
        {
            "$lookup": {
                "from": "supplier",
                "localField": "lineitems.l_suppkey",
                "foreignField": "s_suppkey",
                "as": "suppliers"
            }
        },
        {"$unwind": "$suppliers"},
        {
            "$match": {
                "$expr": {
                    "$eq": ["$suppliers.s_nationkey", "$nations.n_nationkey"]
                }
            }
        },
        {
            "$group": {
                "_id": "$nations.n_name",
                "revenue": {
                    "$sum": {
                        "$multiply": [
                            "$lineitems.l_extendedprice",
                            {"$subtract": [1, "$lineitems.l_discount"]}
                        ]
                    }
                }
            }
        },
        {
            "$project": {
                "_id": 0,
                "n_name": "$_id",
                "revenue": 1
            }
        },
        {
            "$sort": {
                "revenue": -1
            }
        }
    ]
    
    # Q6: Forecasting Revenue Change Query
    queries[6] = [
        {
            "$match": {
                "l_shipdate": {
                    "$gte": datetime(1994, 1, 1),
                    "$lt": datetime(1995, 1, 1)
                },
                "l_discount": {
                    "$gte": 0.05,
                    "$lte": 0.07
                },
                "l_quantity": {"$lt": 24}
            }
        },
        {
            "$group": {
                "_id": None,
                "revenue": {
                    "$sum": {
                        "$multiply": ["$l_extendedprice", "$l_discount"]
                    }
                }
            }
        },
        {
            "$project": {
                "_id": 0,
                "revenue": 1
            }
        }
    ]
    
    # Q7: Volume Shipping Query
    queries[7] = [
        {
            "$lookup": {
                "from": "supplier",
                "localField": "l_suppkey",
                "foreignField": "s_suppkey",
                "as": "suppliers"
            }
        },
        {"$unwind": "$suppliers"},
        {
            "$lookup": {
                "from": "orders",
                "localField": "l_orderkey",
                "foreignField": "o_orderkey",
                "as": "orders"
            }
        },
        {"$unwind": "$orders"},
        {
            "$lookup": {
                "from": "customer",
                "localField": "orders.o_custkey",
                "foreignField": "c_custkey",
                "as": "customers"
            }
        },
        {"$unwind": "$customers"},
        {
            "$lookup": {
                "from": "nation",
                "localField": "suppliers.s_nationkey",
                "foreignField": "n_nationkey",
                "as": "supp_nations"
            }
        },
        {"$unwind": "$supp_nations"},
        {
            "$lookup": {
                "from": "nation",
                "localField": "customers.c_nationkey",
                "foreignField": "n_nationkey",
                "as": "cust_nations"
            }
        },
        {"$unwind": "$cust_nations"},
        {
            "$match": {
                "$or": [
                    {
                        "$and": [
                            {"supp_nations.n_name": "FRANCE"},
                            {"cust_nations.n_name": "GERMANY"}
                        ]
                    },
                    {
                        "$and": [
                            {"supp_nations.n_name": "GERMANY"},
                            {"cust_nations.n_name": "FRANCE"}
                        ]
                    }
                ],
                "l_shipdate": {
                    "$gte": datetime(1995, 1, 1),
                    "$lte": datetime(1996, 12, 31)
                }
            }
        },
        {
            "$project": {
                "supp_nation": "$supp_nations.n_name",
                "cust_nation": "$cust_nations.n_name",
                "l_year": {"$year": "$l_shipdate"},
                "volume": {
                    "$multiply": [
                        "$l_extendedprice",
                        {"$subtract": [1, "$l_discount"]}
                    ]
                }
            }
        },
        {
            "$group": {
                "_id": {
                    "supp_nation": "$supp_nation",
                    "cust_nation": "$cust_nation",
                    "l_year": "$l_year"
                },
                "revenue": {"$sum": "$volume"}
            }
        },
        {
            "$project": {
                "_id": 0,
                "supp_nation": "$_id.supp_nation",
                "cust_nation": "$_id.cust_nation",
                "l_year": "$_id.l_year",
                "revenue": 1
            }
        },
        {
            "$sort": {
                "supp_nation": 1,
                "cust_nation": 1,
                "l_year": 1
            }
        }
    ]
    
    # Q8: National Market Share Query
    queries[8] = [
        {
            "$match": {
                "r_name": "AMERICA"
            }
        },
        {
            "$lookup": {
                "from": "nation",
                "localField": "r_regionkey",
                "foreignField": "n_regionkey",
                "as": "nations"
            }
        },
        {"$unwind": "$nations"},
        {
            "$lookup": {
                "from": "customer",
                "localField": "nations.n_nationkey",
                "foreignField": "c_nationkey",
                "as": "customers"
            }
        },
        {"$unwind": "$customers"},
        {
            "$lookup": {
                "from": "orders",
                "localField": "customers.c_custkey",
                "foreignField": "o_custkey",
                "as": "orders"
            }
        },
        {"$unwind": "$orders"},
        {
            "$match": {
                "orders.o_orderdate": {
                    "$gte": datetime(1995, 1, 1),
                    "$lte": datetime(1996, 12, 31)
                }
            }
        },
        {
            "$lookup": {
                "from": "lineitem",
                "localField": "orders.o_orderkey",
                "foreignField": "l_orderkey",
                "as": "lineitems"
            }
        },
        {"$unwind": "$lineitems"},
        {
            "$lookup": {
                "from": "part",
                "localField": "lineitems.l_partkey",
                "foreignField": "p_partkey",
                "as": "parts"
            }
        },
        {"$unwind": "$parts"},
        {
            "$match": {
                "parts.p_type": "ECONOMY ANODIZED STEEL"
            }
        },
        {
            "$lookup": {
                "from": "supplier",
                "localField": "lineitems.l_suppkey",
                "foreignField": "s_suppkey",
                "as": "suppliers"
            }
        },
        {"$unwind": "$suppliers"},
        {
            "$lookup": {
                "from": "nation",
                "localField": "suppliers.s_nationkey",
                "foreignField": "n_nationkey",
                "as": "supp_nations"
            }
        },
        {"$unwind": "$supp_nations"},
        {
            "$project": {
                "o_year": {"$year": "$orders.o_orderdate"},
                "volume": {
                    "$multiply": [
                        "$lineitems.l_extendedprice",
                        {"$subtract": [1, "$lineitems.l_discount"]}
                    ]
                },
                "nation": "$supp_nations.n_name"
            }
        },
        {
            "$group": {
                "_id": "$o_year",
                "total_volume": {"$sum": "$volume"},
                "brazil_volume": {
                    "$sum": {
                        "$cond": [
                            {"$eq": ["$nation", "BRAZIL"]},
                            "$volume",
                            0
                        ]
                    }
                }
            }
        },
        {
            "$project": {
                "_id": 0,
                "o_year": "$_id",
                "mkt_share": {
                    "$divide": ["$brazil_volume", "$total_volume"]
                }
            }
        },
        {
            "$sort": {
                "o_year": 1
            }
        }
    ]
    
    # Q9: Product Type Profit Measure Query
    queries[9] = [
        {
            "$lookup": {
                "from": "supplier",
                "localField": "l_suppkey",
                "foreignField": "s_suppkey",
                "as": "suppliers"
            }
        },
        {"$unwind": "$suppliers"},
        {
            "$lookup": {
                "from": "partsupp",
                "let": {
                    "suppkey": "$l_suppkey",
                    "partkey": "$l_partkey"
                },
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$ps_suppkey", "$$suppkey"]},
                                    {"$eq": ["$ps_partkey", "$$partkey"]}
                                ]
                            }
                        }
                    }
                ],
                "as": "partsupps"
            }
        },
        {"$unwind": "$partsupps"},
        {
            "$lookup": {
                "from": "part",
                "localField": "l_partkey",
                "foreignField": "p_partkey",
                "as": "parts"
            }
        },
        {"$unwind": "$parts"},
        {
            "$match": {
                "parts.p_name": {"$regex": ".*green.*", "$options": "i"}
            }
        },
        {
            "$lookup": {
                "from": "orders",
                "localField": "l_orderkey",
                "foreignField": "o_orderkey",
                "as": "orders"
            }
        },
        {"$unwind": "$orders"},
        {
            "$lookup": {
                "from": "nation",
                "localField": "suppliers.s_nationkey",
                "foreignField": "n_nationkey",
                "as": "nations"
            }
        },
        {"$unwind": "$nations"},
        {
            "$project": {
                "nation": "$nations.n_name",
                "o_year": {"$year": "$orders.o_orderdate"},
                "amount": {
                    "$subtract": [
                        {
                            "$multiply": [
                                "$l_extendedprice",
                                {"$subtract": [1, "$l_discount"]}
                            ]
                        },
                        {
                            "$multiply": [
                                "$partsupps.ps_supplycost",
                                "$l_quantity"
                            ]
                        }
                    ]
                }
            }
        },
        {
            "$group": {
                "_id": {
                    "nation": "$nation",
                    "o_year": "$o_year"
                },
                "sum_profit": {"$sum": "$amount"}
            }
        },
        {
            "$project": {
                "_id": 0,
                "nation": "$_id.nation",
                "o_year": "$_id.o_year",
                "sum_profit": 1
            }
        },
        {
            "$sort": {
                "nation": 1,
                "o_year": -1
            }
        }
    ]
    
    # Q10: Returned Item Reporting Query
    queries[10] = [
        {
            "$match": {
                "l_returnflag": "R"
            }
        },
        {
            "$lookup": {
                "from": "orders",
                "localField": "l_orderkey",
                "foreignField": "o_orderkey",
                "as": "orders"
            }
        },
        {"$unwind": "$orders"},
        {
            "$match": {
                "orders.o_orderdate": {
                    "$gte": datetime(1993, 10, 1),
                    "$lt": datetime(1994, 1, 1)
                }
            }
        },
        {
            "$lookup": {
                "from": "customer",
                "localField": "orders.o_custkey",
                "foreignField": "c_custkey",
                "as": "customers"
            }
        },
        {"$unwind": "$customers"},
        {
            "$lookup": {
                "from": "nation",
                "localField": "customers.c_nationkey",
                "foreignField": "n_nationkey",
                "as": "nations"
            }
        },
        {"$unwind": "$nations"},
        {
            "$group": {
                "_id": {
                    "c_custkey": "$customers.c_custkey",
                    "c_name": "$customers.c_name",
                    "c_acctbal": "$customers.c_acctbal",
                    "c_phone": "$customers.c_phone",
                    "n_name": "$nations.n_name",
                    "c_address": "$customers.c_address",
                    "c_comment": "$customers.c_comment"
                },
                "revenue": {
                    "$sum": {
                        "$multiply": [
                            "$l_extendedprice",
                            {"$subtract": [1, "$l_discount"]}
                        ]
                    }
                }
            }
        },
        {
            "$project": {
                "_id": 0,
                "c_custkey": "$_id.c_custkey",
                "c_name": "$_id.c_name",
                "revenue": 1,
                "c_acctbal": "$_id.c_acctbal",
                "n_name": "$_id.n_name",
                "c_address": "$_id.c_address",
                "c_phone": "$_id.c_phone",
                "c_comment": "$_id.c_comment"
            }
        },
        {
            "$sort": {
                "revenue": -1
            }
        },
        {"$limit": 20}
    ]
    
    # Q11: Important Stock Identification Query
    queries[11] = [
        {
            "$match": {
                "n_name": "GERMANY"
            }
        },
        {
            "$lookup": {
                "from": "supplier",
                "localField": "n_nationkey",
                "foreignField": "s_nationkey",
                "as": "suppliers"
            }
        },
        {"$unwind": "$suppliers"},
        {
            "$lookup": {
                "from": "partsupp",
                "localField": "suppliers.s_suppkey",
                "foreignField": "ps_suppkey",
                "as": "partsupps"
            }
        },
        {"$unwind": "$partsupps"},
        {
            "$group": {
                "_id": "$partsupps.ps_partkey",
                "value": {
                    "$sum": {
                        "$multiply": [
                            "$partsupps.ps_supplycost",
                            "$partsupps.ps_availqty"
                        ]
                    }
                }
            }
        },
        {
            "$facet": {
                "threshold_calc": [
                    {
                        "$group": {
                            "_id": None,
                            "total_value": {"$sum": "$value"}
                        }
                    },
                    {
                        "$project": {
                            "_id": 0,
                            "threshold": {"$multiply": ["$total_value", 0.0001]}
                        }
                    }
                ],
                "part_values": [
                    {
                        "$project": {
                            "ps_partkey": "$_id",
                            "value": 1
                        }
                    }
                ]
            }
        },
        {"$unwind": "$threshold_calc"},
        {"$unwind": "$part_values"},
        {
            "$match": {
                "$expr": {
                    "$gt": ["$part_values.value", "$threshold_calc.threshold"]
                }
            }
        },
        {
            "$project": {
                "_id": 0,
                "ps_partkey": "$part_values.ps_partkey",
                "value": "$part_values.value"
            }
        },
        {
            "$sort": {
                "value": -1
            }
        }
    ]
    
    # Q12: Shipping Modes and Order Priority Query
    queries[12] = [
        {
            "$match": {
                "l_shipmode": {"$in": ["MAIL", "SHIP"]},
                "$expr": {
                    "$and": [
                        {"$lt": ["$l_commitdate", "$l_receiptdate"]},
                        {"$lt": ["$l_shipdate", "$l_commitdate"]}
                    ]
                },
                "l_receiptdate": {
                    "$gte": datetime(1994, 1, 1),
                    "$lt": datetime(1995, 1, 1)
                }
            }
        },
        {
            "$lookup": {
                "from": "orders",
                "localField": "l_orderkey",
                "foreignField": "o_orderkey",
                "as": "orders"
            }
        },
        {"$unwind": "$orders"},
        {
            "$group": {
                "_id": "$l_shipmode",
                "high_line_count": {
                    "$sum": {
                        "$cond": [
                            {
                                "$or": [
                                    {"$eq": ["$orders.o_orderpriority", "1-URGENT"]},
                                    {"$eq": ["$orders.o_orderpriority", "2-HIGH"]}
                                ]
                            },
                            1,
                            0
                        ]
                    }
                },
                "low_line_count": {
                    "$sum": {
                        "$cond": [
                            {
                                "$and": [
                                    {"$ne": ["$orders.o_orderpriority", "1-URGENT"]},
                                    {"$ne": ["$orders.o_orderpriority", "2-HIGH"]}
                                ]
                            },
                            1,
                            0
                        ]
                    }
                }
            }
        },
        {
            "$project": {
                "_id": 0,
                "l_shipmode": "$_id",
                "high_line_count": 1,
                "low_line_count": 1
            }
        },
        {
            "$sort": {
                "l_shipmode": 1
            }
        }
    ]
    
    # Q13: Customer Distribution Query
    queries[13] = [
        {
            "$lookup": {
                "from": "orders",
                "localField": "c_custkey",
                "foreignField": "o_custkey",
                "as": "orders",
                "pipeline": [
                    {
                        "$match": {
                            "o_comment": {"$not": {"$regex": ".*special.*requests.*", "$options": "i"}}
                        }
                    }
                ]
            }
        },
        {
            "$project": {
                "c_custkey": 1,
                "order_count": {"$size": "$orders"}
            }
        },
        {
            "$group": {
                "_id": "$order_count",
                "custdist": {"$sum": 1}
            }
        },
        {
            "$project": {
                "_id": 0,
                "c_count": "$_id",
                "custdist": 1
            }
        },
        {
            "$sort": {
                "custdist": -1,
                "c_count": -1
            }
        }
    ]
    
    # Q14: Promotion Effect Query
    queries[14] = [
        {
            "$lookup": {
                "from": "part",
                "localField": "l_partkey",
                "foreignField": "p_partkey",
                "as": "parts"
            }
        },
        {"$unwind": "$parts"},
        {
            "$match": {
                "l_shipdate": {
                    "$gte": datetime(1995, 9, 1),
                    "$lt": datetime(1995, 10, 1)
                }
            }
        },
        {
            "$group": {
                "_id": None,
                "promo_revenue": {
                    "$sum": {
                        "$cond": [
                            {"$regexMatch": {"input": "$parts.p_type", "regex": "^PROMO"}},
                            {
                                "$multiply": [
                                    "$l_extendedprice",
                                    {"$subtract": [1, "$l_discount"]}
                                ]
                            },
                            0
                        ]
                    }
                },
                "total_revenue": {
                    "$sum": {
                        "$multiply": [
                            "$l_extendedprice",
                            {"$subtract": [1, "$l_discount"]}
                        ]
                    }
                }
            }
        },
        {
            "$project": {
                "_id": 0,
                "promo_revenue": {
                    "$multiply": [
                        100.0,
                        {"$divide": ["$promo_revenue", "$total_revenue"]}
                    ]
                }
            }
        }
    ]
    
    # Q15: Top Supplier Query
    queries[15] = [
        {
            "$match": {
                "l_shipdate": {
                    "$gte": datetime(1996, 1, 1),
                    "$lt": datetime(1996, 4, 1)
                }
            }
        },
        {
            "$group": {
                "_id": "$l_suppkey",
                "total_revenue": {
                    "$sum": {
                        "$multiply": [
                            "$l_extendedprice",
                            {"$subtract": [1, "$l_discount"]}
                        ]
                    }
                }
            }
        },
        {
            "$group": {
                "_id": None,
                "max_revenue": {"$max": "$total_revenue"}
            }
        },
        {
            "$lookup": {
                "from": "lineitem",
                "pipeline": [
                    {
                        "$match": {
                            "l_shipdate": {
                                "$gte": datetime(1996, 1, 1),
                                "$lt": datetime(1996, 4, 1)
                            }
                        }
                    },
                    {
                        "$group": {
                            "_id": "$l_suppkey",
                            "total_revenue": {
                                "$sum": {
                                    "$multiply": [
                                        "$l_extendedprice",
                                        {"$subtract": [1, "$l_discount"]}
                                    ]
                                }
                            }
                        }
                    },
                    {
                        "$match": {
                            "$expr": {
                                "$eq": ["$total_revenue", "$$max_revenue"]
                            }
                        }
                    },
                    {
                        "$lookup": {
                            "from": "supplier",
                            "localField": "_id",
                            "foreignField": "s_suppkey",
                            "as": "suppliers"
                        }
                    },
                    {"$unwind": "$suppliers"}
                ],
                "let": {"max_revenue": "$max_revenue"},
                "as": "top_suppliers"
            }
        },
        {"$unwind": "$top_suppliers"},
        {
            "$project": {
                "_id": 0,
                "s_suppkey": "$top_suppliers.suppliers.s_suppkey",
                "s_name": "$top_suppliers.suppliers.s_name",
                "s_address": "$top_suppliers.suppliers.s_address",
                "s_phone": "$top_suppliers.suppliers.s_phone",
                "total_revenue": "$top_suppliers.total_revenue"
            }
        },
        {
            "$sort": {
                "s_suppkey": 1
            }
        }
    ]
    
    # Q16: Parts/Supplier Relationship Query
    queries[16] = [
        {
            "$match": {
                "p_brand": {"$ne": "Brand#45"},
                "p_type": {"$not": {"$regex": "^MEDIUM POLISHED"}},
                "p_size": {"$in": [49, 14, 23, 45, 19, 3, 36, 9]}
            }
        },
        {
            "$lookup": {
                "from": "partsupp",
                "localField": "p_partkey",
                "foreignField": "ps_partkey",
                "as": "partsupps"
            }
        },
        {"$unwind": "$partsupps"},
        {
            "$lookup": {
                "from": "supplier",
                "localField": "partsupps.ps_suppkey",
                "foreignField": "s_suppkey",
                "as": "suppliers"
            }
        },
        {"$unwind": "$suppliers"},
        {
            "$match": {
                "suppliers.s_comment": {"$not": {"$regex": ".*Customer.*Complaints.*", "$options": "i"}}
            }
        },
        {
            "$group": {
                "_id": {
                    "brand": "$p_brand",
                    "type": "$p_type",
                    "size": "$p_size"
                },
                "supplier_cnt": {"$addToSet": "$partsupps.ps_suppkey"}
            }
        },
        {
            "$project": {
                "_id": 0,
                "p_brand": "$_id.brand",
                "p_type": "$_id.type",
                "p_size": "$_id.size",
                "supplier_cnt": {"$size": "$supplier_cnt"}
            }
        },
        {
            "$sort": {
                "supplier_cnt": -1,
                "p_brand": 1,
                "p_type": 1,
                "p_size": 1
            }
        }
    ]
    
    # Q17: Small-Quantity-Order Revenue Query
    queries[17] = [
        {
            "$match": {
                "p_brand": "Brand#23",
                "p_container": "MED BOX"
            }
        },
        {
            "$lookup": {
                "from": "lineitem",
                "localField": "p_partkey",
                "foreignField": "l_partkey",
                "as": "lineitems"
            }
        },
        {"$unwind": "$lineitems"},
        {
            "$lookup": {
                "from": "lineitem",
                "let": {"partkey": "$p_partkey"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {"$eq": ["$l_partkey", "$$partkey"]}
                        }
                    },
                    {
                        "$group": {
                            "_id": None,
                            "avg_qty": {"$avg": "$l_quantity"}
                        }
                    }
                ],
                "as": "avg_quantities"
            }
        },
        {"$unwind": "$avg_quantities"},
        {
            "$match": {
                "$expr": {
                    "$lt": [
                        "$lineitems.l_quantity",
                        {"$multiply": [0.2, "$avg_quantities.avg_qty"]}
                    ]
                }
            }
        },
        {
            "$group": {
                "_id": None,
                "avg_yearly": {
                    "$sum": {
                        "$divide": ["$lineitems.l_extendedprice", 7.0]
                    }
                }
            }
        },
        {
            "$project": {
                "_id": 0,
                "avg_yearly": 1
            }
        }
    ]
    
    # Q18: Large Volume Customer Query
    queries[18] = [
        {
            "$group": {
                "_id": "$l_orderkey",
                "total_qty": {"$sum": "$l_quantity"}
            }
        },
        {
            "$match": {
                "total_qty": {"$gt": 300}
            }
        },
        {
            "$lookup": {
                "from": "orders",
                "localField": "_id",
                "foreignField": "o_orderkey",
                "as": "orders"
            }
        },
        {"$unwind": "$orders"},
        {
            "$lookup": {
                "from": "customer",
                "localField": "orders.o_custkey",
                "foreignField": "c_custkey",
                "as": "customers"
            }
        },
        {"$unwind": "$customers"},
        {
            "$lookup": {
                "from": "lineitem",
                "localField": "_id",
                "foreignField": "l_orderkey",
                "as": "lineitems"
            }
        },
        {
            "$group": {
                "_id": {
                    "c_name": "$customers.c_name",
                    "c_custkey": "$customers.c_custkey",
                    "o_orderkey": "$orders.o_orderkey",
                    "o_orderdate": "$orders.o_orderdate",
                    "o_totalprice": "$orders.o_totalprice"
                },
                "sum_qty": {
                    "$sum": "$lineitems.l_quantity"
                }
            }
        },
        {
            "$project": {
                "_id": 0,
                "c_name": "$_id.c_name",
                "c_custkey": "$_id.c_custkey",
                "o_orderkey": "$_id.o_orderkey",
                "o_orderdate": "$_id.o_orderdate",
                "o_totalprice": "$_id.o_totalprice",
                "sum_qty": 1
            }
        },
        {
            "$sort": {
                "o_totalprice": -1,
                "o_orderdate": 1
            }
        },
        {"$limit": 100}
    ]
    
    # Q19: Discounted Revenue Query
    queries[19] = [
        {
            "$lookup": {
                "from": "part",
                "localField": "l_partkey",
                "foreignField": "p_partkey",
                "as": "parts"
            }
        },
        {"$unwind": "$parts"},
        {
            "$match": {
                "$or": [
                    {
                        "$and": [
                            {"parts.p_brand": "Brand#12"},
                            {"parts.p_container": {"$in": ["SM CASE", "SM BOX", "SM PACK", "SM PKG"]}},
                            {"l_quantity": {"$gte": 1, "$lte": 11}},
                            {"parts.p_size": {"$gte": 1, "$lte": 5}},
                            {"l_shipmode": {"$in": ["AIR", "AIR REG"]}},
                            {"l_shipinstruct": "DELIVER IN PERSON"}
                        ]
                    },
                    {
                        "$and": [
                            {"parts.p_brand": "Brand#23"},
                            {"parts.p_container": {"$in": ["MED BAG", "MED BOX", "MED PKG", "MED PACK"]}},
                            {"l_quantity": {"$gte": 10, "$lte": 20}},
                            {"parts.p_size": {"$gte": 1, "$lte": 10}},
                            {"l_shipmode": {"$in": ["AIR", "AIR REG"]}},
                            {"l_shipinstruct": "DELIVER IN PERSON"}
                        ]
                    },
                    {
                        "$and": [
                            {"parts.p_brand": "Brand#34"},
                            {"parts.p_container": {"$in": ["LG CASE", "LG BOX", "LG PACK", "LG PKG"]}},
                            {"l_quantity": {"$gte": 20, "$lte": 30}},
                            {"parts.p_size": {"$gte": 1, "$lte": 15}},
                            {"l_shipmode": {"$in": ["AIR", "AIR REG"]}},
                            {"l_shipinstruct": "DELIVER IN PERSON"}
                        ]
                    }
                ]
            }
        },
        {
            "$group": {
                "_id": None,
                "revenue": {
                    "$sum": {
                        "$multiply": [
                            "$l_extendedprice",
                            {"$subtract": [1, "$l_discount"]}
                        ]
                    }
                }
            }
        },
        {
            "$project": {
                "_id": 0,
                "revenue": 1
            }
        }
    ]
    
    # Q20: Potential Part Promotion Query
    queries[20] = [
        {
            "$match": {
                "p_name": {"$regex": "^forest", "$options": "i"}
            }
        },
        {
            "$lookup": {
                "from": "partsupp",
                "localField": "p_partkey",
                "foreignField": "ps_partkey",
                "as": "partsupps"
            }
        },
        {"$unwind": "$partsupps"},
        {
            "$lookup": {
                "from": "lineitem",
                "let": {
                    "partkey": "$p_partkey",
                    "suppkey": "$partsupps.ps_suppkey"
                },
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$l_partkey", "$$partkey"]},
                                    {"$eq": ["$l_suppkey", "$$suppkey"]},
                                    {"$gte": ["$l_shipdate", datetime(1994, 1, 1)]},
                                    {"$lt": ["$l_shipdate", datetime(1995, 1, 1)]}
                                ]
                            }
                        }
                    },
                    {
                        "$group": {
                            "_id": None,
                            "total_qty": {"$sum": "$l_quantity"}
                        }
                    }
                ],
                "as": "lineitem_totals"
            }
        },
        {"$unwind": "$lineitem_totals"},
        {
            "$match": {
                "$expr": {
                    "$gt": [
                        "$partsupps.ps_availqty",
                        {"$multiply": [0.5, "$lineitem_totals.total_qty"]}
                    ]
                }
            }
        },
        {
            "$lookup": {
                "from": "supplier",
                "localField": "partsupps.ps_suppkey",
                "foreignField": "s_suppkey",
                "as": "suppliers"
            }
        },
        {"$unwind": "$suppliers"},
        {
            "$lookup": {
                "from": "nation",
                "localField": "suppliers.s_nationkey",
                "foreignField": "n_nationkey",
                "as": "nations"
            }
        },
        {"$unwind": "$nations"},
        {
            "$match": {
                "nations.n_name": "CANADA"
            }
        },
        {
            "$project": {
                "_id": 0,
                "s_name": "$suppliers.s_name",
                "s_address": "$suppliers.s_address"
            }
        },
        {
            "$sort": {
                "s_name": 1
            }
        }
    ]
    
    # Q21: Suppliers Who Kept Orders Waiting Query
    queries[21] = [
        {
            "$match": {
                "$expr": {
                    "$gt": ["$l_receiptdate", "$l_commitdate"]
                }
            }
        },
        {
            "$lookup": {
                "from": "orders",
                "localField": "l_orderkey",
                "foreignField": "o_orderkey",
                "as": "orders"
            }
        },
        {"$unwind": "$orders"},
        {
            "$match": {
                "orders.o_orderstatus": "F"
            }
        },
        {
            "$lookup": {
                "from": "supplier",
                "localField": "l_suppkey",
                "foreignField": "s_suppkey",
                "as": "suppliers"
            }
        },
        {"$unwind": "$suppliers"},
        {
            "$lookup": {
                "from": "orders",
                "localField": "l_orderkey",
                "foreignField": "o_orderkey",
                "as": "orders"
            }
        },
        {"$unwind": "$orders"},
        {
            "$lookup": {
                "from": "lineitem",
                "let": {"orderkey": "$l_orderkey", "suppkey": "$l_suppkey"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$l_orderkey", "$$orderkey"]},
                                    {"$ne": ["$l_suppkey", "$$suppkey"]}
                                ]
                            }
                        }
                    }
                ],
                "as": "other_lineitems"
            }
        },
        {
            "$match": {
                "other_lineitems": {"$ne": []}
            }
        },
        {
            "$lookup": {
                "from": "lineitem",
                "let": {"orderkey": "$l_orderkey", "suppkey": "$l_suppkey"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$l_orderkey", "$$orderkey"]},
                                    {"$ne": ["$l_suppkey", "$$suppkey"]},
                                    {"$gt": ["$l_receiptdate", "$l_commitdate"]}
                                ]
                            }
                        }
                    }
                ],
                "as": "late_other_lineitems"
            }
        },
        {
            "$match": {
                "late_other_lineitems": {"$eq": []}
            }
        },
        {
            "$lookup": {
                "from": "nation",
                "localField": "suppliers.s_nationkey",
                "foreignField": "n_nationkey",
                "as": "nations"
            }
        },
        {"$unwind": "$nations"},
        {
            "$match": {
                "nations.n_name": "SAUDI ARABIA"
            }
        },
        {
            "$group": {
                "_id": "$suppliers.s_name",
                "numwait": {"$sum": 1}
            }
        },
        {
            "$project": {
                "_id": 0,
                "s_name": "$_id",
                "numwait": 1
            }
        },
        {
            "$sort": {
                "numwait": -1,
                "s_name": 1
            }
        },
        {"$limit": 100}
    ]
    
    # Q22: Global Sales Opportunity Query
    queries[22] = [
        {
            "$match": {
                "c_phone": {"$regex": "^(13|31|23|29|30|18|17)"},
                "c_acctbal": {"$gt": 0}
            }
        },
        {
            "$group": {
                "_id": None,
                "avg_acctbal": {"$avg": "$c_acctbal"}
            }
        },
        {
            "$lookup": {
                "from": "customer",
                "pipeline": [
                    {
                        "$match": {
                            "c_phone": {"$regex": "^(13|31|23|29|30|18|17)"},
                            "c_acctbal": {"$gt": 0}
                        }
                    },
                    {
                        "$group": {
                            "_id": None,
                            "avg_acctbal": {"$avg": "$c_acctbal"}
                        }
                    },
                    {
                        "$lookup": {
                            "from": "customer",
                            "pipeline": [
                                {
                                    "$match": {
                                        "c_phone": {"$regex": "^(13|31|23|29|30|18|17)"}
                                    }
                                },
                                {
                                    "$match": {
                                        "$expr": {
                                            "$gt": ["$c_acctbal", "$$avg_acctbal"]
                                        }
                                    }
                                },
                                {
                                    "$lookup": {
                                        "from": "orders",
                                        "localField": "c_custkey",
                                        "foreignField": "o_custkey",
                                        "as": "orders"
                                    }
                                },
                                {
                                    "$match": {
                                        "orders": {"$eq": []}
                                    }
                                },
                                {
                                    "$project": {
                                        "cntrycode": {"$substr": ["$c_phone", 0, 2]},
                                        "c_acctbal": 1
                                    }
                                }
                            ],
                            "let": {"avg_acctbal": "$avg_acctbal"},
                            "as": "filtered_customers"
                        }
                    },
                    {"$unwind": "$filtered_customers"},
                    {
                        "$group": {
                            "_id": "$filtered_customers.cntrycode",
                            "numcust": {"$sum": 1},
                            "totacctbal": {"$sum": "$filtered_customers.c_acctbal"}
                        }
                    }
                ],
                "let": {"avg_acctbal": "$avg_acctbal"},
                "as": "results"
            }
        },
        {"$unwind": "$results"},
        {
            "$project": {
                "_id": 0,
                "cntrycode": "$results._id",
                "numcust": "$results.numcust",
                "totacctbal": "$results.totacctbal"
            }
        },
        {
            "$sort": {
                "cntrycode": 1
            }
        }
    ]
    
    return queries


def get_collection_for_query(query_num):
    """Determine which collection to use for a given query."""
    # Most queries start from lineitem
    lineitem_queries = {1, 3, 4, 5, 6, 7, 9, 10, 12, 14, 15, 17, 18, 19, 21}
    region_queries = {2, 5, 8}
    customer_queries = {13, 22}
    part_queries = {16, 20}
    orders_queries = {4, 21}
    
    if query_num in lineitem_queries:
        return "lineitem"
    elif query_num in region_queries:
        return "region"
    elif query_num in customer_queries:
        return "customer"
    elif query_num in part_queries:
        return "part"
    elif query_num in orders_queries:
        return "orders"
    else:
        return "lineitem"  # Default


def run_query(client, db_name, query_num):
    """Run a MongoDB aggregation query and return execution time in milliseconds."""
    import time
    
    queries = get_mongodb_queries()
    
    if query_num not in queries:
        # Return 0 to indicate query is not implemented (bash script checks for > 0)
        return 0
    
    db = client[db_name]
    collection_name = get_collection_for_query(query_num)
    collection = db[collection_name]
    
    start_time = time.time()
    try:
        list(collection.aggregate(queries[query_num], allowDiskUse=True))
    except Exception as e:
        print(f"Error running query {query_num}: {e}", file=sys.stderr)
        return 0
    end_time = time.time()
    
    return (end_time - start_time) * 1000  # Convert to milliseconds


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: benchmark-mongodb-queries.py <host> <port> <database> <query_num>")
        sys.exit(1)
    
    host = sys.argv[1]
    port = int(sys.argv[2])
    db_name = sys.argv[3]
    query_num = int(sys.argv[4])
    
    client = MongoClient(f"mongodb://{host}:{port}/")
    
    duration = run_query(client, db_name, query_num)
    if duration is None:
        sys.exit(1)
    print(f"{duration:.2f}")
