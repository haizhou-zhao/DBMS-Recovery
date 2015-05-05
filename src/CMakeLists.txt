#add all source files
file( GLOB SRCS *.cpp )

#output
add_executable( DBMS_Recovery ${SRCS} ${HEADERS} )