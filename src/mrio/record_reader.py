def record_iter(file_):
    for record in file_:
        yield record[:-1].split('\t')
