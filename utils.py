from cell import Cell

def get_single_node(node, tag):
    res = node.findall(tag)
    if len(res) > 1:
        raise NameError('Multiple node found.')
    elif len(res) == 0:
        return None
    return res[0]

def get_cell_rows(node, row_tag, cell_tag):
    retlist = []
    row_node_list = node.iter(row_tag)
    for row_node in row_node_list:
        celllist = []
        for cell in row_node.iter(cell_tag):
            celllist.append(Cell(xmlnode=cell))
        retlist.append(celllist)
    return retlist
