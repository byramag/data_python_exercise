import argparse
from data_processor import processor

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description = 'Input and output file names')
    parser.add_argument('student_file', help='the path to the file of student information')
    parser.add_argument('teacher_file', help='the path to the file of teacher information')
    parser.add_argument('--out', dest='output_file', help='the desired output path for the json report')
    args = parser.parse_args()

    print(args)

    processor.run(args.student_file, args.teacher_file, args.output_file)