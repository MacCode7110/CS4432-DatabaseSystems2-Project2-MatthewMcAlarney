Matthew McAlarney

Student ID: 893999246

Section 1 - Code Compilation and Execution
    There are two ways that this application can be compiled and run:
        1. Through command line:
            a. Download the project
            b. Open a terminal and navigate to the working directory of this project; the working directory is titled "CS4432-Project1-MatthewMcAlarney"
            c. Compile the application through typing the following command: javac Application.java
            d. Run the application using the following command (the "3" corresponds to the BufferPool size used in the test case file as part of initialization): java Application 3
            e. From here, all commands in the test case file can be entered through command line in the order that they are presented. For each command that is entered, the application outputs a response message.
        2. Through IntelliJ:
            a. Download the project and open it in IntelliJ.
            b. Open a terminal in IntelliJ and make sure that you cd into the working directory of this project; the working directory is titled "CS4432-Project1-MatthewMcAlarney"
            c. In IntelliJ, go to Run -> Edit Configurations
            d. In Edit Configurations:
                I. Make sure that the Name field lists Application.
                II. Under Build and Run, make sure that Application is listed and 3 is entered as the argument to the main method in the Application class.
                III. Click OK to confirm the above configurations.
            e. Go to Run, and under the dropdown menu, click "Run 'Application'"
            f. A console session titled "Application" will pop up under the "Run" tab in IntelliJ, and the application will output the first message, "The program is ready for the next command."
            g. From here, all commands in the test case file can be entered in the console session in the order that they are presented. For each command that is entered, the application outputs a response message.

Section 2 - Test Results

Below are the results of running the test commands in terminal in the same order that they are presented in the test case file.
All the test commands pass.

PS C:\Users\17818\cs4432databasesystems2projects\CS4432-Project1-MatthewMcAlarney> javac Application.java
PS C:\Users\17818\cs4432databasesystems2projects\CS4432-Project1-MatthewMcAlarney> java Application 3
The program is ready for the next command.
Set 430 "F05-Rec450, Jane Do, 10 Hill Rd, age020."
Write was successful; Brought file 5 from disk; Placed in Frame 1
Get 430
F05-Rec450, Jane Do, 10 Hill Rd, age020.; File 5 already in memory; Located in Frame 1
Get 20
F01-Rec020, Name020, address020, age020.; Brought file 1 from disk; Placed in Frame 2
Set 430 "F05-Rec450, John Do, 23 Lake Ln, age056."
Write was successful; File 5 already in memory; Located in Frame 1
Pin 5
File 5 pinned in Frame 1; Not already pinned
Unpin 3
The corresponding block 3 cannot be unpinned because it is not in memory
Get 430
F05-Rec450, John Do, 23 Lake Ln, age056.; File 5 already in memory; Located in Frame 1
Pin 5
File 5 pinned in Frame 1; Already pinned
Get 646
F07-Rec646, Name646, address646, age646.; Brought file 7 from disk; Placed in Frame 3
Pin 3
File 3 pinned in Frame 2; Not already pinned; Evicted file 1 from Frame 2
Set 10 "F01-Rec010, Tim Boe, 09 Deer Dr, age009."
Write was successful; Brought file 1 from disk; Placed in Frame 3; Evicted file 7 from Frame 3
Unpin 1
File 1 in frame 3 is unpinned; Frame 3 was already unpinned
Get 355
F04-Rec355, Name355, address355, age355.; Brought file 4 from disk; Placed in Frame 3; Evicted file 1 from Frame 3
Pin 2
File 2 pinned in Frame 3; Not already pinned; Evicted file 4 from Frame 3
Get 156
F02-Rec156, Name156, address156, age156.; File 2 already in memory; Located in Frame 3
Set 10 "F01-Rec010, No Work, 31 Hill St, age100."
The corresponding block 1 cannot be accessed from disk because the memory buffers are full; Write was unsuccessful
Pin 7
The corresponding block 7 cannot be pinned because the memory buffers are full
Get 10
The corresponding block 1 cannot be accessed from disk because the memory buffers are full
Unpin 3
File 3 is unpinned in frame 2; Frame 2 was not already unpinned
Unpin 2
File 2 is unpinned in frame 3; Frame 3 was not already unpinned
Get 10
F01-Rec010, Tim Boe, 09 Deer Dr, age009.; Brought file 1 from disk; Placed in Frame 2; Evicted file 3 from Frame 2
Pin 6
File 6 pinned in Frame 3; Not already pinned; Evicted file 2 from Frame 3
Exit
Program Exited.

Section 3 - Additional Design Decisions
    In the Application class:
        1. I included the option to exit the program. When the user types "Exit" into the terminal or console session, the message "Program Exited." will be displayed and the program will terminate.
    In the BufferPool class:
        1. I included an additional field titled "indexOfLastEvictedFrame", which is used to keep track of the index of the frame that was last evicted. This field serves an important role in the process of selecting the next frame in the buffer pool that can be evicted.
        2. I included an additional helper method titled "searchForNextFrameToEvict", which uses a simple algorithm and access to the indexOfLastEvictedFrame field to determine the next frame that can be evicted.
            a. This helper method prevents the potential for duplicate code by serving GET, SET, and PIN requests.
        3. I included a helper method titled "obtainFilePath", which returns the appropriate file path for a given blockId.
            a. This method supports repeated read operations of blocks from disk.
        4. I included a helper method titled "obtainFileNumber", which returns the file number of the file that a given record number is located in.
            a. This method supports the retrieval of the file number/blockId in GET, SET, PIN, and UNPIN requests.
        5. I included a helper method titled "obtainModifiedRecordNumber", which returns a modified record number falling in the range of 1 to 100 given a raw record number that the user inputs.
            a. This method provides the needed record number information for getting a record from a block through the Frame class.
            b. For example, in retrieving record number 250, the Frame class needs to look for the 50th record in block 3. The modified record number in this case would be 50 (250 - 200), and this is the number that is used to access the correct index of the record in block 3.
        6. I included a number of helper methods for constructing and displaying messages for the outcomes of the GET, SET, PIN, and UNPIN commands. These helper methods handle message construction for all the listed cases for command scenarios.
            a. printAccessFailure
            b. printGETResult
            c. printSETResult
            d. printPINResult
            e. printUNPINResult