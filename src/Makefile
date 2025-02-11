CXX = g++
CXXFLAGS = -Wall -lpthread -Iinclude

SRC = main.cpp mappers.cpp reducers.cpp threading.cpp
TARGET = map_reduce

all: build

build: $(TARGET)

$(TARGET):
	$(CXX) $(CXXFLAGS) $(SRC) -o $(TARGET)

clean:
	rm -f $(TARGET)
