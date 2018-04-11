from etl_tools import config


def chunk_parameter_function() -> [(str)]:
    """Returns all chunks. Meant to be used in chunking-based parallel tasks"""
    return [(chunk,) for chunk in range(0, config.number_of_chunks())]