import ast

ENV_CHECK = ast.If(
    test=ast.Compare(
        left=ast.Attribute(
            value=ast.Name(id="context", ctx=ast.Load()),
            attr="env",
            ctx=ast.Load(),
        ),
        ops=[ast.NotEq()],
        comparators=[ast.Constant(value="local")],
    ),
    body=[ast.Return(value=None)],
    orelse=[],
)


class InjectEnvCheck(ast.NodeTransformer):
    """AST transformer to inject an environment check into the SparkHooks class."""

    def visit_ClassDef(self, node):
        """Visit class definitions to find SparkHooks and modify its methods.

        Args:
            node (ast.ClassDef): The class definition node.

        Returns:
            ast.ClassDef: The modified class definition node.
        """
        # Process class body (methods)
        if node.name != "SparkHooks":
            return node

        new_body = []
        for item in node.body:
            if (
                isinstance(item, ast.FunctionDef)
                and item.name == "after_context_created"
            ):
                # Insert check at beginning of function body
                # Determine insertion point:
                # If first node is a docstring expression, insert after it.
                insert_pos = 0
                if (
                    len(item.body) > 0
                    and isinstance(item.body[0], ast.Expr)
                    and isinstance(item.body[0].value, ast.Constant)
                    and isinstance(item.body[0].value.value, str)  # it is a docstring
                ):
                    insert_pos = 1

                # Don't double-insert if it's already there
                if len(item.body) > insert_pos and isinstance(
                    item.body[insert_pos], ast.If
                ):
                    return node
                item.body.insert(insert_pos, ENV_CHECK)
            new_body.append(item)

        node.body = new_body
        return node


def transform_spark_hook(path: str):
    """Reads a Python file, injects an environment check into the SparkHooks class,
    and returns the modified source code as a string.

    Args:
        path (str): The file path to the Python source file to be transformed.

    Returns:
        str: The transformed source code with the environment check injected.
    """
    with open(path) as f:
        source = f.read()

    tree = ast.parse(source)
    tree = InjectEnvCheck().visit(tree)
    ast.fix_missing_locations(tree)
    new_source = ast.unparse(tree)

    with open(path, "w") as f:
        f.write(new_source)
    return new_source
