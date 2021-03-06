import collections
import numbers
import copy
import itertools
import functools

ENUM_LIMIT = 5


class Schema(object):
    root = None

    def __init__(self, root):
        self.root = root
        self.definitions = set()

    def to_json(self):
        schema_json = self.root.to_json()
        # add a label of the metaschema version
        schema_json["$schema"] = "http://json-schema.org/draft-07/schema#"
        # if there are any definitions, include them
        if len(self.definitions) > 0:
            schema_json["definitions"] = {}
            for definition in self.definitions:
                schema_json["definitions"][definition.name] \
                    = definition.to_json()
        return schema_json

    def generate_all_nodes(self):
        stack = [self.root]
        stack.extend(self.definitions)
        while len(stack) > 0:
            next_node = stack.pop()
            yield next_node
            if isinstance(next_node, SchemaNodeDict):
                for child in next_node.children:
                    stack.append(child)
            elif isinstance(next_node, SchemaNodeArray):
                for child in next_node.children:
                    stack.append(child)

    def _ref_node_pairs(self):
        """
        generator of pairs of nodes that are equal
        except for name

        these are suitable for replacement with a common reference
        """
        # check if there are any nodes that could be reused
        nodes = sorted(self.generate_all_nodes())
        for i in range(len(nodes)):
            node_a = nodes[i]
            for j in range(i+1, len(nodes)):
                node_b = nodes[j]
                # only care about comparing non-leaf nodes
                # equality includes name but
                # here we don't care about name, only content
                if isinstance(node_a, SchemaNodeArray) and \
                        isinstance(node_b, SchemaNodeArray):
                    if node_a.children == node_b.children:
                        yield sorted((node_a, node_b))
                elif isinstance(node_a, SchemaNodeDict) and \
                        isinstance(node_b, SchemaNodeDict):
                    if node_a.required == node_b.required and \
                            node_a.children == node_b.children:
                        yield sorted((node_a, node_b))

    def _ref_components(self):
        """
        Generate sets of nodes that could be replaced with a single reference.
        """
        node_sets = set()
        for node in self.generate_all_nodes():
            node_sets.add(frozenset((node,)))

        for node_a, node_b in self._ref_node_pairs():
            node_set_a = None
            node_set_b = None

            for node_set in node_sets:
                if node_a in node_set:
                    node_set_a = node_set
                if node_b in node_set:
                    node_set_b = node_set

            assert node_set_a is not None
            assert node_set_b is not None

            if node_set_a is not node_set_b:
                node_sets.remove(node_set_a)
                node_sets.remove(node_set_b)
                node_sets.add(node_set_a.union(node_set_b))

        return frozenset(node_sets)

    def infer_references(self):
        changed = True
        while changed:
            changed = False

            # combine all pairwise matches into bigger groups
            components = self._ref_components()
            # filter only to components of more than one node
            components = [x for x in components if len(x) > 1]
            if len(components) == 0:
                continue
            # pick the biggest group, defined as total number of nodes
            biggest_component = None
            biggest_component_len = -1
            for component in components:
                component_len = sum((len(x) for x in component))
                if component_len > biggest_component_len:
                    biggest_component = component
            assert biggest_component is not None

            if len(biggest_component) > 1:
                # take any single node as an example
                # they should all be the same except for name, so its fine
                example, *_ = biggest_component
                # create a new node based on the example with a combined name
                # ideally this sort of internal-only name should be human
                # choosable, but not sure how to do that
                # for now, combine existing names
                definition_name = "_".join(
                        sorted(set(x.name for x in biggest_component)))
                example = copy.deepcopy(example)
                example.name = definition_name

                # add the node to the definition
                # TODO check no definition with the same name exists already
                self.definitions.add(example)

                # take each of the nodes in the biggest component
                # replace them with a reference node

                nodes = tuple(self.generate_all_nodes())
                for node in nodes:
                    if isinstance(node, SchemaNodeDict):
                        new_children = []
                        for child in tuple(node.children):
                            if child in biggest_component:
                                new_children.append(
                                    SchemaNodeRef(child.name,
                                                  definition_name))
                            else:
                                new_children.append(child)
                        node.children = tuple(new_children)
                    elif isinstance(node, SchemaNodeArray):
                        new_children = []
                        for child in tuple(node.children):
                            if child in biggest_component:
                                new_children.append(
                                    SchemaNodeRef(child.name,
                                                  definition_name))
                            else:
                                new_children.append(child)
                        node.children = tuple(new_children)

                changed = True

    def merge(self, other):
        if other is None:
            return self
        elif self.root is None and other.root is not None:
            return other
        elif self.root is not None and other.root is None:
            return self

        merged = Schema(self.root.merge(other.root))
        merged.definitions = self.definitions.union(other.definitions)
        return merged

    @classmethod
    def schema_extractor(clazz, thing):
        root = SchemaNode.discover_class(thing) \
                .from_json_instance(thing, None)
        # TODO calculate coocurance matrix
        return clazz(root)


class SchemaNode(object):
    name = None

    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return 'SchemaNode({})'.format(
            self.name)

    def __str__(self):
        return 'SchemaNode({})'.format(
            self.name)

    def __eq__(self, other):
        if not issubclass(other.__class__, self.__class__):
            return False
        if self.name != other.name:
            return False
        return True

    def __lt__(self, other):
        raise NotImplementedError()

    def __hash__(self):
        return hash((self.name,))

    def __len__(self):
        return 1

    def to_json(self):
        raise NotImplementedError()

    def merge(self, other):
        raise NotImplementedError()

    @classmethod
    def from_json_instance(clazz, obj, name):
        raise NotImplementedError()

    @classmethod
    def discover_class(clazz, thing):
        if isinstance(thing, collections.abc.Mapping):
            return SchemaNodeDict
        # if its a list, iterate
        # exclude strings because they are iterable by character
        elif isinstance(thing, collections.abc.Iterable) \
                and not isinstance(thing, str):
            return SchemaNodeArray
        else:
            return SchemaNodeLeaf


@functools.total_ordering
class SchemaNodeDict(SchemaNode):
    children = frozenset()
    required = frozenset()

    def __init__(self, name, children, required):
        super().__init__(name)
        assert isinstance(children, collections.abc.Iterable), \
            "children must be iterable"
        assert isinstance(required, collections.abc.Iterable), \
            "required must be iterable"
        self.children = frozenset(children)
        self.required = frozenset(required)

    def __eq__(self, other):
        if self is other:
            return True
        if not issubclass(other.__class__, self.__class__):
            return False
        if self.name != other.name:
            return False
        if self.required != other.required:
            return False
        if self.children != other.children:
            return False
        return True

    def __lt__(self, other):
        if self.name is None and other.name is not None:
            return True
        elif self.name is not None and other.name is None:
            return False
        elif self.name < other.name:
            return True
        elif self.name > other.name:
            return False

        if self.children < other.children:
            return True
        elif self.children > other.children:
            return False

        if self.required < other.required:
            return True
        elif self.required > other.required:
            return False

        return False

    def __hash__(self):
        return hash((self.name, self.required, self.children))

    def __len__(self):
        return sum((len(x) for x in self.children))+1

    def __repr__(self):
        return 'SchemaNodeDict({}, {}, {})'.format(
            self.name, self.children, self.required)

    def __str__(self):
        return 'SchemaNodeDict({}, {}, {})'.format(
            self.name, self.children, self.required)

    @classmethod
    def from_json_instance(clazz, thing, name=None):
        assert isinstance(thing, collections.abc.Mapping)
        children = set()
        for key in sorted(thing.keys()):
            # determine which node type this object should be
            # represented as, then delegate to that node type to build
            # an appropriate node
            child = clazz.discover_class(thing[key]) \
                    .from_json_instance(thing[key], key)
            children.add(child)
        children = frozenset(children)
        # assume that everything is required to start with
        # this will be relaxed when merging
        required = frozenset((x.name for x in children))
        return SchemaNodeDict(name, children, required)

    def to_json(self):
        json = {}
        json["type"] = "object"
        json["properties"] = {}
        for child in self.children:
            json["properties"][child.name] = child.to_json()
        if len(self.required) > 0:
            json["required"] = sorted(self.required)
        return json

    def merge(self, other):
        if other is None:
            return self
        assert isinstance(other, SchemaNode)
        assert self.__class__ == other.__class__
        assert self.name == other.name

        children = set()
        self_childnames = frozenset([x.name for x in self.children])
        other_childnames = frozenset([x.name for x in other.children])
        childnames = sorted(self_childnames | other_childnames)

        for childname in childnames:
            try:
                self_child = [x for x in self.children
                              if x.name == childname][0]
            except IndexError:
                self_child = None
            try:
                other_child = [x for x in other.children
                               if x.name == childname][0]
            except IndexError:
                other_child = None

            if self_child is None:
                assert other_child is not None
                children.add(other_child)
            elif other_child is None:
                assert self_child is not None
                children.add(self_child)
            else:
                children.add(self_child.merge(other_child))

        # things can be marked as required iff they are required in both
        required = self.required & other.required

        return SchemaNodeDict(self.name, children, required)


@functools.total_ordering
class SchemaNodeArray(SchemaNode):
    def __init__(self, name, children):
        super().__init__(name)
        assert isinstance(children, collections.abc.Iterable), \
            "children must be iterable"
        self.children = tuple(children)

    def __eq__(self, other):
        if self is other:
            return True
        if not issubclass(other.__class__, self.__class__):
            return False
        if self.name != other.name:
            return False
        if self.children != other.children:
            return False
        return True

    def __lt__(self, other):
        if self.name is None and other.name is not None:
            return True
        elif self.name is not None and other.name is None:
            return False
        elif self.name < other.name:
            return True
        elif self.name > other.name:
            return False

        if self.children < other.children:
            return True
        elif self.children > other.children:
            return False

        return False

    def __hash__(self):
        return hash((self.name, self.children))

    def __len__(self):
        return sum((len(x) for x in self.children))+1

    def __repr__(self):
        return 'SchemaNodeArray({}, {})'.format(
            self.name, self.children)

    def __str__(self):
        return 'SchemaNodeArray({}, {})'.format(
            self.name, self.children)

    @classmethod
    def from_json_instance(clazz, thing, name=None):
        assert isinstance(thing, collections.abc.Iterable)
        children = []
        for thing_child in thing:
            # determine which node type this object should be
            # represented as, then delegate to that node type to build
            # an appropriate node
            child = clazz.discover_class(thing_child) \
                    .from_json_instance(thing_child, None)
            children.append(child)
        children = frozenset(children)
        return SchemaNodeArray(name, children)

    def to_json(self):
        json = {}
        json["type"] = "array"
        if len(self.children) > 0:
            # merge children together
            # TODO what if position matters?
            children_merged = self.children[0]
            for child in self.children[1:]:
                children_merged = children_merged.merge(child)
            json["items"] = children_merged.to_json()
        # TODO minlen maxlen
        # TODO uniqueItems
        return json

    def merge(self, other):
        if other is None:
            return self
        assert isinstance(other, SchemaNodeArray)
        assert self.name == other.name

        children = []
        for self_child, other_child in itertools.zip_longest(
                    self.children, other.children):
            if self_child is None:
                assert other_child is not None
                children.append(other_child)
            elif other_child is None:
                assert self_child is not None
                children.append(self_child)
            else:
                children.append(self_child.merge(other_child))

        return SchemaNodeArray(self.name, children)


@functools.total_ordering
class SchemaNodeLeaf(SchemaNode):
    def __init__(self, name, values, datatype):
        assert values is None or isinstance(
                values, collections.abc.Collection), \
                "values must be collection or None"
        super().__init__(name)
        if values is None:
            self.values = None
        else:
            self.values = frozenset(values)
        # TODO check the datatype is sensible
        self.datatype = datatype

    def __eq__(self, other):
        if self is other:
            return True
        if not issubclass(other.__class__, self.__class__):
            return False
        if self.name != other.name:
            return False
        if self.datatype != other.datatype:
            return False
        if self.values != other.values:
            return False
        return True

    def __lt__(self, other):
        if self.name is None and other.name is not None:
            return True
        elif self.name is not None and other.name is None:
            return False
        elif self.name < other.name:
            return True
        elif self.name > other.name:
            return False

        if self.datatype < other.datatype:
            return True
        elif self.datatype > other.datatype:
            return False

        if self.values is None and other.values is not None:
            return True
        elif self.values is not None and other.values is None:
            return False
        elif self.values is None and other.values is None:
            pass
        elif self.values < other.values:
            return True
        elif self.values > other.values:
            return False

        return False

    def __hash__(self):
        return hash((self.name, self.datatype, self.values))

    def __len__(self):
        return 1

    def __repr__(self):
        return 'SchemaNodeLeaf({}, {}, {})'.format(
            self.name, sorted(self.values), self.datatype)

    def __str__(self):
        return 'SchemaNodeLeaf({}, {}, {})'.format(
            self.name, sorted(self.values), self.datatype)

    @classmethod
    def from_json_instance(clazz, thing, name=None):
        if isinstance(thing, str):
            return SchemaNodeLeaf(name, [thing], "string")
        elif isinstance(thing, bool):
            return SchemaNodeLeaf(name, [thing], "boolean")
        elif isinstance(thing, int):
            return SchemaNodeLeaf(name, [thing], "integer")
        elif isinstance(thing, numbers.Number):
            return SchemaNodeLeaf(name, [thing], "number")
        elif thing is None:
            return SchemaNodeLeaf(name, [thing], "null")
        else:
            # shouldn't get here so raise an exception in case
            raise ValueError("Unrecognized thing {}".format(thing))

    def to_json(self):
        json = {}

        if self.datatype is not None:
            json["type"] = self.datatype
            # if there a few unique values, its an enum
            # otherwise, its free values and we can't store all
            if self.values is not None:
                if len(self.values) == 1:
                    json["const"] = tuple(self.values)[0]
                else:
                    json["enum"] = sorted((x for x in self.values))
            elif self.datatype == "string":
                # TODO min_length
                # TODO max_length
                # TODO format
                # TODO pattern ?
                pass
            elif self.datatype == "integer":
                # TODO maximum / exclusiveMaximum
                # TODO minimum / exclusiveMinimum
                # TODO multipleOf ?
                pass
            elif self.datatype == "number":
                # TODO maximum / exclusiveMaximum
                # TODO minimum / exclusiveMinimum
                # TODO multipleOf ?
                pass

            # TODO other data types
        return json

    def merge(self, other):
        if other is None:
            return self
        assert isinstance(other, SchemaNodeLeaf), \
            "{} must be SchemaNodeLeaf".format(other.name)
        assert self.name == other.name

        if other.datatype != self.datatype:
            child_datatype = None
        else:
            child_datatype = self.datatype

        # merge values
        if self.values is None or other.values is None:
            child_values = None
        else:
            child_values = set()
            for value in self.values:
                # TODO warn for zero-length keys
                child_values.add(value)
            for value in other.values:
                # TODO warn for zero-length keys
                child_values.add(value)

            # if we now have too many different values, don't be enum
            if len(child_values) > ENUM_LIMIT:
                child_values = None

        return SchemaNodeLeaf(self.name, child_values, child_datatype)


class SchemaNodeRef(SchemaNode):
    def __init__(self, name, ref):
        super().__init__(name)
        self.ref = ref

    def __eq__(self, other):
        if not issubclass(other.__class__, self.__class__):
            return False
        if self.name != other.name:
            return False
        if self.ref != other.ref:
            return False
        return True

    def __lt__(self, other):
        if self.name is None and other.name is not None:
            return True
        elif self.name is not None and other.name is None:
            return False
        elif self.name < other.name:
            return True
        elif self.name > other.name:
            return False

        if self.ref < other.ref:
            return True
        elif self.ref > other.ref:
            return False

        return False

    def __hash__(self):
        return hash((self.name, self.ref))

    def __repr__(self):
        return 'SchemaNodeRef({}, {})'.format(
            self.name, self.ref)

    def __str__(self):
        return 'SchemaNodeRef({}, {})'.format(
            self.name, self.ref)

    def to_json(self):
        json = {}
        json["$ref"] = '#/definitions/{}'.format(self.ref)
        return json
