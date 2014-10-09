class Cell(object):
    """ Atomic unit with content and layout information in a table. """

    def __init__(self, xmlnode=None):
        if xmlnode is not None:
            self.content = xmlnode.text
            self.datatype = 'text'
        else:
            self.content = None
            self.datatype = None

    def __str__(self):
        if self.content is not None:
            return self.content.encode('utf-8')
        else:
            return u'_'
