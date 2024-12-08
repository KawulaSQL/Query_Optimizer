from model.models import ParsedQuery, QueryTree
from helper.get_object import get_operator_operands_from_condition
from helper.validation import validate_string
from typing import List, Optional, Set

def generate_query_plans(optimizer_instance, tree: QueryTree) -> List[QueryTree]:
    if not tree:
        return []

    plans = [tree]
    
    if tree.type == ["sigma", "product"]:
        plans.extend(apply_selection_rules(optimizer_instance, tree))
    elif tree.type in ["join", "natural join", "theta join"]:
        plans.extend(apply_join_rules(optimizer_instance, tree))
    elif tree.type == "project":
        plans.extend(apply_projection_rules(optimizer_instance, tree))
        
    return plans

def apply_selection_rules(optimizer_instance, tree: QueryTree) -> List[QueryTree]:
    plans = []
    
    # Rule 1
    if tree.condition and " AND " in tree.condition:
        conditions = tree.condition.split(" AND ")
        current_tree = None
        
        for condition in conditions:
            new_tree = QueryTree(
                type="sigma",
                val=tree.val,
                condition=condition.strip(),
                child=[current_tree if current_tree else tree.child[0]]
            )
            if current_tree:
                current_tree.parent = new_tree
            current_tree = new_tree
        plans.append(current_tree)
    
    # Rule 2
    if len(tree.child) > 0 and tree.child[0].type == "sigma":
        swapped_tree = QueryTree(
            type="sigma",
            val=tree.child[0].val,
            condition=tree.child[0].condition,
            child=[
                QueryTree(
                    type="sigma",
                    val=tree.val,
                    condition=tree.condition,
                    child=tree.child[0].child
                )
            ]
        )
        plans.append(swapped_tree)
        
    # Rule 4a
    if len(tree.child) > 0 and tree.child[0].type == "product":
        new_tree = QueryTree(
            type="product",
            val=tree.child[0].val,
            condition=f"{tree.child[0].condition} AND {tree.condition}",
            child=tree.child[0].child
        )
        plans.append(new_tree)
        
    # Rule 4b
    if len(tree.child) > 0 and tree.child[0].type == "theta join":
        join_node = tree.child[0]
        new_tree = QueryTree(
            type="theta join",
            val=join_node.val,
            condition=f"{join_node.condition} AND {tree.condition}",
            child=join_node.child
        )
        plans.append(new_tree) 
    
    # Rule 7
    if len(tree.child) > 0 and tree.child[0].type == "join":
        join_node = tree.child[0]
        
        left_attrs = get_relation_attributes(optimizer_instance, join_node.child[0])
        right_attrs = get_relation_attributes(optimizer_instance, join_node.child[1])
        
        if condition_only_uses_attributes(optimizer_instance, tree.condition, left_attrs):
            new_tree = QueryTree(
                type="join",
                val=join_node.val,
                condition=join_node.condition,
                child=[
                    QueryTree(type="sigma", val=tree.val, condition=tree.condition, child=[join_node.child[0]]),
                    join_node.child[1]
                ]
            )
            plans.append(new_tree)
            
        elif condition_only_uses_attributes(optimizer_instance, tree.condition, right_attrs):
            new_tree = QueryTree(
                type="join",
                val=join_node.val,
                condition=join_node.condition,
                child=[
                    join_node.child[0],
                    QueryTree(type="sigma", val=tree.val, condition=tree.condition, child=[join_node.child[1]])
                ]
            )
            plans.append(new_tree)
            
    return plans

def apply_join_rules(optimizer_instance, tree: QueryTree) -> List[QueryTree]:
    plans = []
    
    if len(tree.child) == 2:
        # Rule 5
        swapped_join = QueryTree(
            type=tree.type,
            val=tree.val,
            condition=tree.condition,
            child=[tree.child[1], tree.child[0]]
        )
        plans.append(swapped_join)
        
        # Rule 6
        if tree.type == "natural join" and tree.child[0].type == "natural join":
            # (A ⋈ B) ⋈ C → A ⋈ (B ⋈ C)
            left_join = tree.child[0]
            reassociated = QueryTree(
                type="natural join",
                val=tree.val,
                condition="",
                child=[
                    left_join.child[0],
                    QueryTree(
                        type="natural join",
                        val=left_join.val,
                        condition="",
                        child=[left_join.child[1], tree.child[1]]
                    )
                ]
            )
            plans.append(reassociated)
            
    return plans

def apply_projection_rules(optimizer_instance, tree: QueryTree) -> List[QueryTree]:
    plans = []
    
    # Rule 3
    if len(tree.child) > 0 and tree.child[0].type == "project":
        simplified = QueryTree(
            type="project",
            val=tree.val,
            condition=tree.condition,
            child=tree.child[0].child
        )
        plans.append(simplified)
        
    # Rule 8
    if len(tree.child) > 0 and tree.child[0].type == "join":
        join_node = tree.child[0]
        projection_attrs = set(tree.condition.split(","))
        
        left_attrs = set(get_relation_attributes(optimizer_instance, join_node.child[0]))
        right_attrs = set(get_relation_attributes(optimizer_instance, join_node.child[1]))
        
        left_proj = projection_attrs.intersection(left_attrs)
        right_proj = projection_attrs.intersection(right_attrs)
        
        if left_proj and right_proj:
            distributed = QueryTree(
                type="join",
                val=join_node.val,
                condition=join_node.condition,
                child=[
                    QueryTree(type="project", val=tree.val, condition=",".join(left_proj), child=[join_node.child[0]]),
                    QueryTree(type="project", val=tree.val, condition=",".join(right_proj), child=[join_node.child[1]])
                ]
            )
            plans.append(distributed)
            
    return plans

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