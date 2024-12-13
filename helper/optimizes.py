from model.models import ParsedQuery, QueryTree
from helper.get_object import get_operator_operands_from_condition
from helper.validation import validate_string
from typing import List, Optional, Set

def optimize_tree(optimizer_instance, tree: QueryTree) -> List[QueryTree]:
    if not tree:
        return []
    
    optimized_trees = [tree]

    for i in range(len(tree.child)):
        child_optimizations = optimize_tree(optimizer_instance, tree.child[i])
        # print(optimized_trees)
        if child_optimizations:
            for opt_child in child_optimizations:
                # print("opt_child:", opt_child)
                new_tree = copy_tree(optimizer_instance, tree)
                new_tree.child[i] = opt_child
                opt_child.parent = new_tree
                optimized_trees.append(new_tree)
    
    if tree.type == "sigma":
        optimized_trees.extend(push_down_selection(optimizer_instance, tree))
    elif tree.type == "project":
        optimized_trees.extend(push_down_projection(optimizer_instance, tree))
    elif tree.type in ["join", "natural join"]:
        optimized_trees.extend(optimize_joins(optimizer_instance, tree))

    return optimized_trees

def push_down_selection(optimizer_instance, tree: QueryTree) -> List[QueryTree]:    
    if not tree.child:
        return []
    
    optimized_trees = []
    child = tree.child[0]

    if tree.type == "sigma":
        sigma_conditions = []
        current = tree
        while current and current.type == "sigma":
            sigma_conditions.append(current.condition)
            current = current.child[0] if current.child else None
        
        base_node = current
        
        if base_node:
            new_tree = copy_tree(optimizer_instance, base_node)
            
            condition_table_map = {}
            for condition in sigma_conditions:
                condition_attrs = extract_attributes(optimizer_instance, condition)
                condition_table = condition_attrs.pop().split('.')[0] if condition_attrs else None
                if condition_table:
                    if condition_table not in condition_table_map:
                        condition_table_map[condition_table] = []
                    condition_table_map[condition_table].append(condition)
            
            def push_sigmas_to_matching_table(node: QueryTree) -> QueryTree:
                if not node:
                    return None
                
                if node.type == "table":
                    table_name = node.val.strip()
                    if table_name in condition_table_map:
                        # Apply all conditions for this table
                        current_node = node
                        for condition in condition_table_map[table_name]:
                            current_node = QueryTree(
                                type="sigma",
                                val="",
                                condition=condition,
                                child=[current_node]
                            )
                        return current_node
                    return node
                
                if node.type in ["join", "natural join"]:
                    new_node = copy_tree(optimizer_instance, node)
                    new_node.child[0] = push_sigmas_to_matching_table(node.child[0])
                    new_node.child[1] = push_sigmas_to_matching_table(node.child[1])
                    return new_node
                
                return node
            
            optimized_tree = push_sigmas_to_matching_table(new_tree)
            if optimized_tree:
                optimized_trees.append(optimized_tree)
    
    else:
        for i in range(len(tree.child)):
            child_optimizations = push_down_selection(optimizer_instance, tree.child[i])
            if child_optimizations:
                for opt_child in child_optimizations:
                    new_tree = copy_tree(optimizer_instance, tree)
                    new_tree.child[i] = opt_child
                    opt_child.parent = new_tree
                    optimized_trees.append(new_tree)

    return optimized_trees

def optimize_joins(optimizer_instance, tree: QueryTree) -> List[QueryTree]:
    if len(tree.child) != 2:
        return []
    
    optimized_trees = []

    joins = optimizer_instance._extract_joins(tree)
    if len(joins) >= 2:
        join_orders = optimizer_instance._generate_join_orders(joins)
        for order in join_orders:
            new_tree = optimizer_instance._build_join_tree(order)
            optimized_trees.append(new_tree)
    
    return optimized_trees

def generate_join_orders(optimizer_instance, joins: List[tuple]) -> List[List[tuple]]:
    if len(joins) <= 1:
        return [joins]
    
    all_orders = []
    for i in range(len(joins)):
        current = joins[i]
        remaining = joins[:i] + joins[i+1:]

        for sub_order in generate_join_orders(optimizer_instance, remaining):
            all_orders.append([current] + sub_order)
    
    return all_orders

def build_join_tree(optimizer_instance, joins: List[tuple]) -> QueryTree:
    if not joins:
        return None
    
    current_join = joins[0]
    left_child, right_child, condition = current_join

    tree = QueryTree(
        type="join",
        val="",
        condition=condition,
        child=[left_child, right_child]
    )

    for join in joins[1:]:
        left, right, condition = join
        tree = QueryTree(
            type="join",
            val="",
            condition=condition,
            child=[tree, right]
        )
    return tree

def push_down_projection(optimizer_instance, tree: QueryTree) -> List[QueryTree]:
    if not tree.child:
        return []
    
    optimized_trees = []
    child = tree.child[0]

    if child.type == "project":
        # 8a
        new_tree = QueryTree(
            type="project",
            val=tree.val,
            condition=tree.condition, 
            child=child.child 
        )
        optimized_trees.append(new_tree)

    elif child.type == ["join", "natural join"]:
        projection_attrs = set(tree.condition.split(","))
        left_attrs = set(get_relation_attributes(optimizer_instance, child.child[0]))
        right_attrs = set(get_relation_attributes(optimizer_instance, child.child[1]))

        join_condition_attrs = extract_join_attributes(child.condition)

        # left attribute
        l1 = projection_attrs.intersection(left_attrs)
        # right attribute
        l2 = projection_attrs.intersection(right_attrs)

        # left attr tapi tidak l1 U l2
        l3 = {attr for attr in join_condition_attrs if attr in left_rel_attrs and attr not in (l1.union(l2))}
        # right attr tapi tidak l1 U l2
        l4 = {attr for attr in join_condition_attrs if attr in right_rel_attrs and attr not in (l1.union(l2))}

        # 8a
        if l1 and l2:
            new_tree = QueryTree(
                type="join",
                val=child.val,
                condition=child.condition,
                child=[
                    QueryTree(type="project", val=tree.val, condition=",".join(left_proj), child=[child.child[0]]),
                    QueryTree(type="project", val=tree.val, condition=",".join(right_proj), child=[child.child[1]])
                ]
            )
            optimized_trees.append(new_tree)
        else:
            # 8b
            left_proj_attrs = l1.union(l3)
            right_proj_attrs = l2.union(l4)

            new_tree = QueryTree(
                type="project",
                val=tree.val,
                condition=tree.condition,
                child=[
                    QueryTree(
                        type="join",
                        val=child.val,
                        condition=child.condition,
                        child=[
                            QueryTree(type="project", val=tree.val, 
                                    condition=",".join(left_proj_attrs), 
                                    child=[child.child[0]]),
                            QueryTree(type="project", val=tree.val, 
                                    condition=",".join(right_proj_attrs), 
                                    child=[child.child[1]])
                        ]
                    )
                ]
            )
            optimized_trees.append(new_tree)
    else:
        new_tree = QueryTree(
            type="project",
            val=tree.val,
            condition=tree.condition,
            child=[child]
        )
        optimized_trees.append(new_tree)

    return optimized_trees

def extract_join_attributes(condition: str) -> Set[str]:
    attrs = set()
    try:
        if not condition:
            return attrs
            
        conditions = condition.split(" AND ")
        
        for cond in conditions:
            operator, left_operand, right_operand = get_operator_operands_from_condition(cond)
            
            for operand in [left_operand, right_operand]:
                if not (operand.isnumeric() or validate_string(operand)):
                    if "." in operand:
                        attrs.add(operand)
                    else:
                        attrs.add(operand)
                        
        return attrs
    except:
        return attrs

def optimize_joins(optimizer_instance, tree: QueryTree) -> List[QueryTree]:
    if len(tree.child) != 2:
        return []
    
    optimized_trees = []

    joins = extract_joins(optimizer_instance, tree)
    if (len(joins) >= 2):
        join_orders = generate_join_orders(optimizer_instance, joins)
        for order in join_orders:
            new_tree = build_join_tree(optimizer_instance, order)
            optimized_trees.append(new_tree)
    
    return optimized_trees

def extract_joins(optimizer_instance, tree: QueryTree) -> List[tuple]:
    joins = []
    if tree.type == ["join", "natural join"]:
        joins.append((tree.child[0], tree.child[1], tree.condition))

        for child in tree.child:
            if child.type == ["join", "natural join"]:
                joins.extend(extract_joins(optimizer_instance, child))
    
    return joins

def copy_tree(optimizer_instance, tree:QueryTree) -> QueryTree:
    if not tree:
        return None
    
    new_tree = QueryTree(
        type=tree.type,
        val=tree.val,
        condition=tree.condition,
        child=[],
        total_block=tree.total_block,
        total_row=tree.total_row,
        columns=tree.columns.copy() if tree.columns else []
    )

    for child in tree.child:
        child_copy = copy_tree(optimizer_instance, child)
        if child_copy:
            new_tree.child.append(child_copy)
            child_copy.parent = new_tree
    
    return new_tree

def extract_attributes(optimizer_instance, condition: str) -> Set[str]:
    attrs = set()
    try:
        operator, left_operand, right_operand = get_operator_operands_from_condition(condition)
        
        for operand in [left_operand, right_operand]:
            if not (operand.isnumeric() or validate_string(operand)):
                if "." in operand:
                    attrs.add(operand)
                else:
                    attrs.add(operand)
        return attrs
    except:
        return attrs

def get_relation_attributes(optimizer_instance, node: QueryTree) -> List[str]:
    return node.columns if hasattr(node, 'columns') else []

def condition_only_uses_attributes(optimizer_instance, condition: str, attributes: List[str]) -> bool:
    try:
        operator, left_operand, right_operand = get_operator_operands_from_condition(condition)
        left_attr = left_operand.split('.')[-1] if '.' in left_operand else left_operand
        right_attr = right_operand.split('.')[-1] if '.' in right_operand else right_operand
        
        for attr in [left_attr, right_attr]:
            if not (attr.isnumeric() or validate_string(attr)):
                if not any(attr in a for a in attributes):
                    return False
        return True
    except:
        return False