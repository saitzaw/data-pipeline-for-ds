#include <iostream>
#include <cmath>
using namespace std;

int main(int argc, char* argv[])
{
    // hardcoded matrices values 
    double A[2][2] = {{1.0, 3.0}, {4.0, -2.0}}; 
    double B[2][2] = {{2.0, -1.0}, {0.0, 2.0}};
    double C[2][2]; 
    int rows = *(&A[0] + 1) - A[0];
    int cols = *(&A[1] + 1) - A[1];
    for (int i=0; i < rows; i++)
    {
        for (int j=0; j < cols; j++)
        {
            C[i][j] = 0;
            for (int k=0; k < cols; k++)
            {
                C[i][j] += A[i][k]*B[k][j];
            }
        }
    }

     for (int i=0; i < rows; i++)
    {
        for (int j=0; j < cols; j++)
        {
            cout << C[i][j] << " " ;
        }
            cout << endl;
    }
    return 0;
}