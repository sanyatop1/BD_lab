from model import authors
from model import books
from model import readers
from model import abonements
from view import view

v = view()
au = authors()
b = books()
r = readers()
ab = abonements()

command = view.readCommand()

if (command == 'create'):

    table = view.readTable()

    if (table == 'authors'):
        print('Enter name, country')
        name = view.getVal()
        country = view.getVal()
        au.create(name, country)
    elif (table == 'books'):
        print('Enter title, price, author_id, abonement_id')
        title = view.getVal()
        price = view.getInt()
        author_id = view.getInt()
        abonement_id = view.getInt()
        b.create(title, price, author_id, abonement_id)
    elif (table == 'readers'):
        print('Enter username, read_count, abonement_id')
        username = view.getVal()
        read_count = view.getInt()
        abonement_id = view.getInt()
        r.create(username, read_count, abonement_id)
    elif (table == 'abonements'):
        print('Enter price')
        price = view.getInt()
        ab.create(price)
    else:
        print('Invalid table name')

elif (command == 'read'):
    table = view.readTable()

    if (table == 'authors'):
        print('Enter id')
        id = view.getInt()
        au.read(id)
    elif (table == 'books'):
        print('Enter id')
        id = view.getInt()
        b.read(id)
    elif (table == 'readers'):
        print('Enter id')
        id = view.getInt()
        r.read(id)
    elif (table == 'abonements'):
        print('Enter id')
        id = view.getInt()
        ab.read(id)
    else:
        print('Invalid table name')

elif (command == 'update'):

    table = view.readTable()

    if (table == 'authors'):
        print('Enter id, new name, new country')
        id = view.getInt()
        name = view.getVal()
        country = view.getVal()
        au.update(id, name, country)
    elif (table == 'books'):
        print('Enter id, new title, new year, new author_id, new abonement_id')
        id = view.getInt()
        title = view.getVal()
        year = view.getInt()
        author_id = view.getInt()
        abonement_id = view.getInt()
        b.update(id, title, year, author_id, abonement_id)
    elif (table == 'readers'):
        print('Enter id, new username, new read_count, new aboniment_id')
        id = view.getInt()
        username = view.getVal()
        read_count = view.getInt()
        abonement_id = view.getInt()
        r.update(id, username, read_count, abonement_id)
    elif (table == 'abonements'):
        print('Enter id, new price')
        id = view.getInt()
        price = view.getInt()
        ab.update(id, price)
    else:
        print('Invalid table name')

elif (command == 'delete'):

    table = view.readTable()

    if (table == 'authors'):
        print('Enter id')
        id = view.getInt()
        au.delete(id)
    elif (table == 'books'):
        print('Enter id')
        id = view.getInt()
        b.delete(id)
    elif (table == 'readers'):
        print('Enter id')
        id = view.getInt()
        r.delete(id)
    elif (table == 'abonements'):
        print('Enter id')
        id = view.getInt()
        ab.delete(id)
    else:
        print('Invalid table name')

elif (command == 'generate'):

    table = view.readTable()

    if (table == 'authors'):
        print('Enter numeber to generate')
        gen_num = view.getInt()
        au.generate(gen_num)
    elif (table == 'books'):
        print('Enter numeber to generate')
        gen_num = view.getInt()
        b.generate(gen_num)
    elif (table == 'readers'):
        print('Enter numeber to generate')
        gen_num = view.getInt()
        r.generate(gen_num)
    elif (table == 'abonements'):
        print('Enter numeber to generate')
        gen_num = view.getInt()
        ab.generate(gen_num)
    else:
        print('Invalid table name')

elif (command == 'search'):
    print('Enter year, name')
    year = view.getInt()
    name = view.getVal()

    au.searchAuthorsBooks(name, year)

else:
    print('Invalid command')