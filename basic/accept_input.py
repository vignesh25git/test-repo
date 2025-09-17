def accept_input() :
    a = input("Enter input branch 1")
    return a

def convert_to_integer(str):
    num = int(str)
    return num

if __name__ == '__main__':
    print('hello')
    a=accept_input()
    print(f'entered values is {a}')
    try:
        num=convert_to_integer(a)
    except Exception as e:
        print(f'Exception occured {e}')
    else:
        print(f'no error successfully converted the string to numeric')
    finally:
        print("hello finally")





