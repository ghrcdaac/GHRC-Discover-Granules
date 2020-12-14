class File:
    filename = ''
    link = ''
    date_modified = ''
    time_modified = ''
    meridiem_modified = ''

    def __init__(self, filename: str = '', link: str = '', date: str = '', time: str = '', meridiem: str = ''):
        """
        Default values goes here
        """
        self.filename = filename
        self.link = link
        self.date_modified = date
        self.time_modified = time
        self.meridiem_modified = meridiem
