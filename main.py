<<<<<<< HEAD
pass
print("Hello")
=======
from add import add
from sub import sub
from multiply import multiply
from divide import divide
def calculator():
    a = float(input("첫 번째 숫자: "))
    b = float(input("두 번째 숫자: "))
    op = input("연산자 (+, -, *, /): ")
    operations = {
    '+': add,
    '-': sub,
    '*': multiply,
    '/': divide
 }
    if op in operations:
        try:
            result = operations[op](a, b)
            print("결과:", result)
        except Exception as e:
            print("오류:", e)
    else:
        print("올바르지 않은 연산자입니다.")
if __name__ == "__main__":
    calculator()
>>>>>>> main
