#include <iostream>
#include <cmath>
using namespace std;


int main(int argc, char* argv[])
{
    // hardcoded array
    double arr[] = {1.0, 2.0, 4.0, 2.0, 5.0, 7.0, 8.0, 10.0, 11.0, 2.0};
    double avg=0.0;

    //find the length of array
    int arrSize = sizeof(arr)/sizeof(arr[0]);

    // sum all values in array
    for (int i=0; i<arrSize; i++)
    {
        avg+=arr[i];
    }
    cout << "elements in array are: " << endl;

    // output for the array
    for (int i=0; i<arrSize; i++)
    {
        cout << arr[i] << " ";
    }
    cout << endl;
    cout << "size of array is: " << arrSize << endl;
    cout << "avearge or mean value of array is: " << avg/double(arrSize) << endl;
    return 0;
}