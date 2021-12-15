class view:

    def __init__(self):
        print('Program started')

    def readCommand():
        com = input("Enter command (create, read, update, delete, generate, search): ")
        return com

    def readTable():
        tab = input("Enter table name (authors, books, abonements, readers): ")
        if (tab == 'authors' or tab == 'books' or tab == 'abonements' or tab == 'readers'):
            return tab
        return ''

    def getVal():
        val = input("Enter string: ")
        return val

    def getInt():
        val = input("Enter integer: ")
        try:
            int(val)
        except ValueError:
            print(val + " is not integer")
            return 0
        return int(val)