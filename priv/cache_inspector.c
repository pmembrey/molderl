
//==============================================================================
//
// Compile:
// $ gcc -Wall -o cache_inspector cache_inspector.c
//
// Usage:
// $ ./cache_inspector < molderl.cache
//
// Requirements:
// * work on unix systems
//
//==============================================================================

#include<stdio.h>
#include <unistd.h>

int main() {

    ssize_t nbytes;
    unsigned char buf[2];
    unsigned int packet_length;
    unsigned int packet_counter = 0;
    unsigned int minimum = 20000; // ~2x bigger than jumbo frames MTU, to be safe
    unsigned int maximum = 0;
    unsigned int total_size = 0;

    do { // loop over MoldUDP64 packets
        nbytes = read(STDIN_FILENO, buf, 2);
        packet_length = ((int)buf[0] << 8) | buf[1];
        total_size += packet_length;
        minimum = packet_length < minimum ? packet_length : minimum;
        maximum = packet_length > maximum ? packet_length : maximum;
        ++packet_counter;
        lseek(STDIN_FILENO, packet_length, SEEK_CUR);
    } while (nbytes != 0);

    printf("number of packets: %u\n", packet_counter);
    printf("total data (bytes): %u\n", total_size);
    printf("smallest packet (bytes): %u\n", minimum);
    printf("avg packet size (bytes): %u\n", total_size/packet_counter);
    printf("largest packet (bytes): %u\n", maximum);
    return 0;
}

