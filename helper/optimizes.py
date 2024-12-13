from model.models import ParsedQuery, QueryTree
from helper.get_object import get_operator_operands_from_condition
from helper.validation import validate_string
from typing import List, Optional, Set

# def generate_query_plans(optimizer_instance, tree: QueryTree) -> List[QueryTree]:
#     if not tree:
#         return []

#     plans = [tree]
    
#     if tree.type == ["sigma", "product"]:
#         plans.extend(apply_selection_rules(optimizer_instance, tree))
#     elif tree.type in ["join", "natural join", "theta join"]:
#         plans.extend(apply_join_rules(optimizer_instance, tree))
#     elif tree.type == "project":
#         plans.extend(apply_projection_rules(optimizer_instance, tree))
        
#     return plans

# def generate_query_plans(optimizer_instance, tree: QueryTree) -> List[QueryTree]:
#     if not tree:
#         return []

#     plans = [tree]
    
#     if tree.type == "sigma":
#         plans.extend(optimizer_instance._apply_selection_rules(tree))
#     elif tree.type == "join":
#         plans.extend(optimizer_instance._apply_join_rules(tree))
#     # elif tree.type == "project":
#     #     plans.extend(optimizer_instance._apply_projection_rules(tree))
    
#     for plan in list(plans):
#         for i, child in enumerate(plan.child):
#             child_plans = optimizer_instance._generate_query_plans(child)
#             for child_plan in child_plans:
#                 new_plan = copy.deepcopy(plan)
#                 new_plan.child[i] = child_plan
#                 new_plan.child[i].parent = new_plan
#                 plans.append(new_plan)
    
#     return plans

# def types_list(optimizer_instance, tree: QueryTree) -> List[str]:
#     list_table = []
#     list_sigma = []
#     list_join = []
#     if len(tree) > 0:
#         if (tree.type == "sigma"):
#             list_sigma.append(tree)
#         elif (tree.type == "join"):
#             list_join.append(tree)
#         elif (tree.type == "table"):
#             list_table.append(tree)
#         else:
#             for child in tree.child:
#                 list_table.extend(optimizer_instance.types_list(child))

#     return list_table

def optimize_tree(optimizer_instance, tree: QueryTree) -> List[QueryTree]:
    if not tree:
        return []
    
    optimized_trees = [tree]

    for i in range(len(tree.child)):
        child_optimizations = optimize_tree(optimizer_instance, tree.child[i])
        if child_optimizations:
            for opt_child in child_optimizations:
                new_tree = copy_tree(optimizer_instance, tree)
                new_tree.child[i] = opt_child
                opt_child.parent = new_tree
                optimized_trees.append(new_tree)
    
    if tree.type == "sigma":
        optimized_trees.extend(push_down_selection(optimizer_instance, tree))
    elif tree.type == "project":
        optimized_trees.extend(push_down_projection(optimizer_instance, tree))
    elif tree.type == "join":
        optimized_trees.extend(optimize_joins(optimizer_instance, tree))

    return optimized_trees

def push_down_selection(optimizer_instance, tree: QueryTree) -> List[QueryTree]:    
    if not tree.child:
        return []
    
    optimized_trees = []
    child = tree.child[0]

    if child.type == "join":
        left_attrs = set(get_relation_attributes(optimizer_instance, child.child[0]))
        right_attrs = set(get_relation_attributes(optimizer_instance, child.child[1]))
        condition_attrs = extract_attributes(optimizer_instance, tree.condition)

        if all(attr in left_attrs for attr in condition_attrs):
            left_child = child.child[0]
            if left_child.type == "join":
                left_sigma = QueryTree(
                    type="sigma",
                    val=tree.val,
                    condition=tree.condition,
                    child=[left_child]
                )
                pushed_sigmas = push_down_selection(optimizer_instance, left_sigma)
                
                for pushed_sigma in pushed_sigmas:
                    new_tree = QueryTree(
                        type="join",
                        val=child.val,
                        condition=child.condition,
                        child=[pushed_sigma, child.child[1]]
                    )
                    optimized_trees.append(new_tree)
            else:
                new_tree = QueryTree(
                    type="join",
                    val=child.val,
                    condition=child.condition,
                    child=[
                        QueryTree(type="sigma", val=tree.val, condition=tree.condition, 
                                child=[left_child]),
                        child.child[1]
                    ]
                )
                optimized_trees.append(new_tree)

        elif all(attr in right_attrs for attr in condition_attrs):
            right_child = child.child[1]
            if right_child.type == "join":
                right_sigma = QueryTree(
                    type="sigma",
                    val=tree.val,
                    condition=tree.condition,
                    child=[right_child]
                )
                pushed_sigmas = push_down_selection(optimizer_instance, right_sigma)
                
                for pushed_sigma in pushed_sigmas:
                    new_tree = QueryTree(
                        type="join",
                        val=child.val,
                        condition=child.condition,
                        child=[child.child[0], pushed_sigma]
                    )
                    optimized_trees.append(new_tree)
            else:
                new_tree = QueryTree(
                    type="join",
                    val=child.val,
                    condition=child.condition,
                    child=[
                        child.child[0],
                        QueryTree(type="sigma", val=tree.val, condition=tree.condition, 
                                child=[right_child])
                    ]
                )
                optimized_trees.append(new_tree)
    
    elif child.type == "sigma":
        new_tree = QueryTree(
            type="sigma",
            val=tree.val,
            condition=f"{tree.condition} AND {child.condition}",
            child=child.child
        )
        optimized_trees.extend(push_down_selection(optimizer_instance, new_tree))

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

    if child.type == "join":
        projection_attrs = set(tree.condition.split(","))

        left_attrs = set(get_relation_attributes(optimizer_instance, child.child[0]))
        right_attrs = set(get_relation_attributes(optimizer_instance, child.child[1]))

        left_proj = projection_attrs.intersection(left_attrs)
        right_proj = projection_attrs.intersection(right_attrs)

        if left_proj and right_proj:
            new_tree = QueryTree(
                type="join",
                val=child.val,
                condition=child.condition,
                child=[
                    QueryTree(type="project", val=tree.val, condition=",".join(left_proj), child=[child.child[0]])
                ]
            )
            optimized_trees.append(new_tree)
    
    return optimized_trees

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
    if tree.type == "join":
        joins.append((tree.child[0], tree.child[1], tree.condition))

        for child in tree.child:
            if child.type == "join":
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
    

# def apply_selection_rules(optimizer_instance, tree: QueryTree) -> List[QueryTree]:
#     plans = []
    
#     # Rule 1
#     if tree.condition and " AND " in tree.condition:
#         conditions = tree.condition.split(" AND ")
#         current_tree = None
        
#         for condition in conditions:
#             new_tree = QueryTree(
#                 type="sigma",
#                 val=tree.val,
#                 condition=condition.strip(),
#                 child=[current_tree if current_tree else tree.child[0]]
#             )
#             if current_tree:
#                 current_tree.parent = new_tree
#             current_tree = new_tree
#         plans.append(current_tree)
    
#     # Rule 2
#     if len(tree.child) > 0 and tree.child[0].type == "sigma":
#         swapped_tree = QueryTree(
#             type="sigma",
#             val=tree.child[0].val,
#             condition=tree.child[0].condition,
#             child=[
#                 QueryTree(
#                     type="sigma",
#                     val=tree.val,
#                     condition=tree.condition,
#                     child=tree.child[0].child
#                 )
#             ]
#         )
#         plans.append(swapped_tree)
        
#     # Rule 4a
#     if len(tree.child) > 0 and tree.child[0].type == "product":
#         new_tree = QueryTree(
#             type="product",
#             val=tree.child[0].val,
#             condition=f"{tree.child[0].condition} AND {tree.condition}",
#             child=tree.child[0].child
#         )
#         plans.append(new_tree)
        
#     # Rule 4b
#     if len(tree.child) > 0 and tree.child[0].type == "theta join":
#         join_node = tree.child[0]
#         new_tree = QueryTree(
#             type="theta join",
#             val=join_node.val,
#             condition=f"{join_node.condition} AND {tree.condition}",
#             child=join_node.child
#         )
#         plans.append(new_tree) 
    
#     # Rule 7
#     if len(tree.child) > 0 and tree.child[0].type == "join":
#         join_node = tree.child[0]
        
#         left_attrs = get_relation_attributes(optimizer_instance, join_node.child[0])
#         right_attrs = get_relation_attributes(optimizer_instance, join_node.child[1])
        
#         if condition_only_uses_attributes(optimizer_instance, tree.condition, left_attrs):
#             new_tree = QueryTree(
#                 type="join",
#                 val=join_node.val,
#                 condition=join_node.condition,
#                 child=[
#                     QueryTree(type="sigma", val=tree.val, condition=tree.condition, child=[join_node.child[0]]),
#                     join_node.child[1]
#                 ]
#             )
#             plans.append(new_tree)
            
#         elif condition_only_uses_attributes(optimizer_instance, tree.condition, right_attrs):
#             new_tree = QueryTree(
#                 type="join",
#                 val=join_node.val,
#                 condition=join_node.condition,
#                 child=[
#                     join_node.child[0],
#                     QueryTree(type="sigma", val=tree.val, condition=tree.condition, child=[join_node.child[1]])
#                 ]
#             )
#             plans.append(new_tree)
            
#     return plans

# def apply_join_rules(optimizer_instance, tree: QueryTree) -> List[QueryTree]:
#     plans = []
    
#     if len(tree.child) == 2:
#         # Rule 5
#         swapped_join = QueryTree(
#             type=tree.type,
#             val=tree.val,
#             condition=tree.condition,
#             child=[tree.child[1], tree.child[0]]
#         )
#         plans.append(swapped_join)
        
#         # Rule 6
#         if tree.type == "natural join" and tree.child[0].type == "natural join":
#             # (A ⋈ B) ⋈ C → A ⋈ (B ⋈ C)
#             left_join = tree.child[0]
#             reassociated = QueryTree(
#                 type="natural join",
#                 val=tree.val,
#                 condition="",
#                 child=[
#                     left_join.child[0],
#                     QueryTree(
#                         type="natural join",
#                         val=left_join.val,
#                         condition="",
#                         child=[left_join.child[1], tree.child[1]]
#                     )
#                 ]
#             )
#             plans.append(reassociated)
            
#     return plans

# def apply_projection_rules(optimizer_instance, tree: QueryTree) -> List[QueryTree]:
#     plans = []
    
#     # Rule 3
#     if len(tree.child) > 0 and tree.child[0].type == "project":
#         simplified = QueryTree(
#             type="project",
#             val=tree.val,
#             condition=tree.condition,
#             child=tree.child[0].child
#         )
#         plans.append(simplified)
        
#     # Rule 8
#     if len(tree.child) > 0 and tree.child[0].type == "join":
#         join_node = tree.child[0]
#         projection_attrs = set(tree.condition.split(","))
        
#         left_attrs = set(get_relation_attributes(optimizer_instance, join_node.child[0]))
#         right_attrs = set(get_relation_attributes(optimizer_instance, join_node.child[1]))
#         left_proj = projection_attrs.intersection(left_attrs)
#         right_proj = projection_attrs.intersection(right_attrs)
        
#         if left_proj and right_proj:
#             distributed = QueryTree(
#                 type="join",
#                 val=join_node.val,
#                 condition=join_node.condition,
#                 child=[
#                     QueryTree(type="project", val=tree.val, condition=",".join(left_proj), child=[join_node.child[0]]),
#                     QueryTree(type="project", val=tree.val, condition=",".join(right_proj), child=[join_node.child[1]])
#                 ]
#             )
#             plans.append(distributed)
            
#     return plans

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