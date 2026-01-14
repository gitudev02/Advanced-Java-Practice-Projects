import java.util.Scanner;

public class Calculator {
    private Scanner scanner;
    
    public Calculator() {
        scanner = new Scanner(System.in);
    }
    
    public void run() {
        System.out.println("=================================");
        System.out.println("   Welcome to Java Calculator   ");
        System.out.println("=================================");
        
        while (true) {
            displayMenu();
            int choice = getChoice();
            
            if (choice == 0) {
                System.out.println("Thank you for using the calculator. Goodbye!");
                break;
            }
            
            performOperation(choice);
        }
        
        scanner.close();
    }
    
    private void displayMenu() {
        System.out.println("\nChoose an operation:");
        System.out.println("1. Addition (+)");
        System.out.println("2. Subtraction (-)");
        System.out.println("3. Multiplication (*)");
        System.out.println("4. Division (/)");
        System.out.println("5. Modulus (%)");
        System.out.println("6. Power (^)");
        System.out.println("7. Square Root (√)");
        System.out.println("8. Percentage");
        System.out.println("0. Exit");
        System.out.print("\nEnter your choice: ");
    }
    
    private int getChoice() {
        try {
            return scanner.nextInt();
        } catch (Exception e) {
            scanner.nextLine(); // Clear invalid input
            return -1;
        }
    }
    
    private double getNumber(String prompt) {
        System.out.print(prompt);
        while (!scanner.hasNextDouble()) {
            System.out.print("Invalid input. Please enter a number: ");
            scanner.next();
        }
        return scanner.nextDouble();
    }
    
    private void performOperation(int choice) {
        double num1, num2, result;
        
        switch (choice) {
            case 1: // Addition
                num1 = getNumber("Enter first number: ");
                num2 = getNumber("Enter second number: ");
                result = add(num1, num2);
                System.out.println("\nResult: " + num1 + " + " + num2 + " = " + result);
                break;
                
            case 2: // Subtraction
                num1 = getNumber("Enter first number: ");
                num2 = getNumber("Enter second number: ");
                result = subtract(num1, num2);
                System.out.println("\nResult: " + num1 + " - " + num2 + " = " + result);
                break;
                
            case 3: // Multiplication
                num1 = getNumber("Enter first number: ");
                num2 = getNumber("Enter second number: ");
                result = multiply(num1, num2);
                System.out.println("\nResult: " + num1 + " * " + num2 + " = " + result);
                break;
                
            case 4: // Division
                num1 = getNumber("Enter first number: ");
                num2 = getNumber("Enter second number: ");
                if (num2 == 0) {
                    System.out.println("\nError: Cannot divide by zero!");
                } else {
                    result = divide(num1, num2);
                    System.out.println("\nResult: " + num1 + " / " + num2 + " = " + result);
                }
                break;
                
            case 5: // Modulus
                num1 = getNumber("Enter first number: ");
                num2 = getNumber("Enter second number: ");
                if (num2 == 0) {
                    System.out.println("\nError: Cannot perform modulus with zero!");
                } else {
                    result = modulus(num1, num2);
                    System.out.println("\nResult: " + num1 + " % " + num2 + " = " + result);
                }
                break;
                
            case 6: // Power
                num1 = getNumber("Enter base number: ");
                num2 = getNumber("Enter exponent: ");
                result = power(num1, num2);
                System.out.println("\nResult: " + num1 + " ^ " + num2 + " = " + result);
                break;
                
            case 7: // Square Root
                num1 = getNumber("Enter number: ");
                if (num1 < 0) {
                    System.out.println("\nError: Cannot calculate square root of negative number!");
                } else {
                    result = squareRoot(num1);
                    System.out.println("\nResult: √" + num1 + " = " + result);
                }
                break;
                
            case 8: // Percentage
                num1 = getNumber("Enter the number: ");
                num2 = getNumber("Enter the percentage: ");
                result = percentage(num1, num2);
                System.out.println("\nResult: " + num2 + "% of " + num1 + " = " + result);
                break;
                
            default:
                System.out.println("\nInvalid choice! Please select a valid option.");
        }
    }
    
    // Basic Operations
    private double add(double a, double b) {
        return a + b;
    }
    
    private double subtract(double a, double b) {
        return a - b;
    }
    
    private double multiply(double a, double b) {
        return a * b;
    }
    
    private double divide(double a, double b) {
        return a / b;
    }
    
    private double modulus(double a, double b) {
        return a % b;
    }
    
    // Advanced Operations
    private double power(double base, double exponent) {
        return Math.pow(base, exponent);
    }
    
    private double squareRoot(double num) {
        return Math.sqrt(num);
    }
    
    private double percentage(double num, double percent) {
        return (num * percent) / 100;
    }
    
    // Main method to run the calculator
    public static void main(String[] args) {
        Calculator calculator = new Calculator();
        calculator.run();
    }
}