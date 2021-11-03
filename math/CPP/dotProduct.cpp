#include <iostream>
#include <cmath>


int main(int argc, char* argv[])
{
    // dot product in C++
    // hardcoded values 

    double vec1[3] = {1.0, -2.0, 3.0};
    double vec2[3] = {2.5, -1.5, -4.0};
    double z = 0.0;
    double norm = 0.0;
    int arrSize = *(&vec1 + 1) - vec1;
    for (int i = 0; i < arrSize; i++)
    {
        z+=vec1[i]*vec2[i];
    }
    std::cout << "dot product of two vectors: " << z << std::endl;

    // vec1 norm
    for (int i = 0; i < arrSize; i++)
    {
        norm += pow(vec1[i],2);
    }
    std::cout << "norm for vec1: " << sqrt(norm) << std::endl;
    norm = 0.0;

    //vec2 norm
        for (int i = 0; i < arrSize; i++)
    {
        norm += pow(vec2[i],2);
    }
    std::cout << "norm for vec1: " << sqrt(norm) << std::endl;
    return 0;

}
