class Granule(dict):
    filename = ''
    link = ''
    date_modified = ''
    time_modified = ''
    meridiem_modified = ''

    def __str__(self):
        return f'{self.link}, {self.filename}, {self.date_modified}, {self.time_modified}, {self.meridiem_modified}'

    def __init__(self, link: str = '', filename: str = '', date: str = '', time: str = '', meridiem: str = ''):
        """
        Default values goes here
        """
        super().__init__()
        dict.__init__(self, link=link, filename=filename, date=date, time=time, meridiem=meridiem)
        self.link = link
        self.filename = filename
        self.date_modified = date
        self.time_modified = time
        self.meridiem_modified = meridiem
