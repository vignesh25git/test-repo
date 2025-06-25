n1 = int(input("Enter Number 1"))
n2 = int(input("Enter Number 2"))

try:
    n = n1/n2
    print(f'{n1} divided by {n2} is {n}')
except Exception as E:
    print(f'exception occured {E}')


