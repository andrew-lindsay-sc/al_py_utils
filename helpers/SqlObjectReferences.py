from email import generator
import re
from anytree import Node, RenderTree

from helpers.StaticMethods import *
from helpers.PrintColors import *
from helpers.SqlObject import *

class SqlObjectReferences:
    def __init__(self, root_object: SqlObject):
        self.refs = dict()
        self._root_object = root_object

        self._children = Node(self._root_object.fully_qualified_name)
        self._parse_children(self._children)

    # TODO: implement this
    def _parse_parents(self, object_name: str, definition: str) -> None:
        raise Exception("Not Implemented Yet")

    def _parse_children(self, base_node: Node) -> None:
        if self._children.name == '':
            self._children = Node(self._root_object.fully_qualified_name)

        references = list(self._get_referenced_objects(base_node.name))
        for reference in references:
            child_node = Node(reference, parent = base_node)
            self._parse_children(child_node)

    def print_children(self) -> None:
        for pre, fill, node in RenderTree(self._children):
            print("%s%s" % (pre, node.name))

    def _add_reference(self, reference, referencee) -> None:
        if reference not in self.refs:
            self.refs[reference] = list()

        self.refs[reference].append(referencee)

    def _get_referenced_objects(self, object_name: str) -> generator[str]:
        obj = SqlObject(f"{object_name}")
        try:
            words = obj.definition.split(' ')
        except:
            print_fail(f"Definition for {object_name} could not be read. Dependency tree will be incomplete. Skipping...")
            return

        object_references = list(filter(lambda w: '${project}' in w and 'temp.' not in w, words))
        for ref in object_references:
            reference = ref.replace('${dataset}', obj.dataset). \
                replace('`','').replace('${color}', 'blue'). \
                    replace("${project}", f"souncommerce-client-{self._root_object.client_name}").\
                        replace('\n', '')

            reference = re.sub(r'([a-zA-Z_0-9\.\-\*]+).*', r'\1', reference)

            if reference == object_name:
                continue

            yield reference

    def _flatten_tree(self, tree: Node) -> None:
        depth = tree.height
        while depth >= 0:
            descendants = list(filter(lambda d: d.depth == depth, tree.descendants))
            for descendant in descendants:
                yield descendant.name

            depth -= 1

    def get_children(self) -> set[str]:
        return set(self._flatten_tree(self._children))