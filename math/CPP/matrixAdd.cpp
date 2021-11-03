#include <iostream>
#include <cmath>
using namespace std;

int main(int argc, char* argv[])
{
    // Add two matrix in C++
    // hardcoded matrices

    double A[2][2] = {{1, 3}, {4, -1}};
    double B[2][2] = {{3, -1}, {3, 0}};
    double C[2][2];
    int rows = *(&A[1] + 1) - A[1];
    int cols = *(&A[2] + 1) - A[2];
    
    for (int i=0; i < rows; i++)
    {
        for (int j=0; j < cols; j++)
        {
            C[i][j] = A[i][j] + B[i][j];
        }
    }

     for (int i=0; i < rows; i++)
    {
        for (int j=0; j < cols; j++)
        {
            cout << C[i][j] << "\t" ;
        }
            cout << endl;
    }

    return 0;
}