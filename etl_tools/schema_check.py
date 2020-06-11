from mara_pipelines import pipelines


class AbortOnSchemaMisuse(pipelines.Command):
    def __init__(self, schema_name: str) -> None:
        """
        Checks that a pipeline does not use the specified schema in any files

        Under the hood, it uses egrep to scan the directory for "schema_name\." .
        It excludes '__init__.py' and *.md files.

        Args:
            schema_name: str, the schema name which should not be used
        """
        self.schema_name = schema_name
        self.pattern = f'{schema_name}\.'

    def run(self):
        from mara_pipelines import shell
        from mara_pipelines.logging import logger
        pipeline_base_directory = self.parent.parent.base_path()
        excludes = ' --exclude=__init__.py --exclude=\*.md --exclude=\*.pyc'
        # cd'ing && grepping in . allows us to show short filenames
        # The "(...) || true" will ensure that we do not get any output if nothing is found
        shell_command = f'(cd "{pipeline_base_directory}" && egrep --recursive {excludes} "{self.pattern}" .) || true'
        lines_or_bool = shell.run_shell_command(shell_command)
        if lines_or_bool is True:
            return True
        else:
            # The || true makes sure we will not get any False
            logger.log(f"Please don\'t use the pattern '{self.pattern}' in this pipeline. Matching lines:",
                       format=logger.Format.ITALICS)
            lines = '\n'.join(lines_or_bool)
            logger.log(f"{lines}", format=logger.Format.ITALICS)
            return False

    def html_doc_items(self) -> [(str, str)]:
        from mara_page import _
        from html import escape
        return [
            ('schema_name', _.pre[escape(self.schema_name)]),
            ('egrep pattern', _.pre[escape(self.pattern)]),
        ]


def add_schema_misuse_check_as_first_command_in_initial_task(pipeline: pipelines.Pipeline):
    """Adds a check to any pipeline with a schema that this schema is not used in any files"""

    def _find_all_pipelines(pipeline: pipelines.Pipeline, all_found_pipelines):
        for node in pipeline.nodes.values():
            if isinstance(node, pipelines.Pipeline) and node not in all_found_pipelines:
                all_found_pipelines.append(node)
                _find_all_pipelines(node, all_found_pipelines)

    all_pipelines = [pipeline]
    _find_all_pipelines(pipeline, all_pipelines)
    for p in all_pipelines:
        if 'Schema' in p.labels:
            schema = p.labels['Schema']
            description = f"Checks that the schema of this pipeline ({schema}) is not used in this pipeline"
            if not p.initial_node:
                p.add_initial(pipelines.Task(f'initial_task_in_{p.id}', description=description))
            else:
                p.initial_node.description += f' + {description}'
            assert isinstance(p.initial_node, pipelines.Task)
            p.initial_node.add_command(AbortOnSchemaMisuse(schema), prepend=True)
