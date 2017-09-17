def main():
    str = input("Enter your input: ");
    print ("Received input is : ", str)
    i = 2
    run = 0
    while i < 1e15:
        i = i * i
        run += 1
    print ("fin i=", i, " run: ", run)


main()
