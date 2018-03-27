-- get most occurence word

lines  = LOAD 'words.txt' AS (line:chararray);

/*

(Paragraph writing is the foundation of all essay writing, whether the form is expository, persuasive, narrative, or creative. In order to write a good paragraph, students need to understand the four essential elements of paragraph writing and how each element contributes to the whole. At Time4Writing, a certified teacher acts as an online writing tutor to help students build writing skills by focusing on the fundamentals. And nothing in the writing process is more fundamental than writing a solid paragraph.)

*/


toknz = foreach lines generate FLATTEN(TOKENIZE((chararray)line)) as word;

/*

	TOKENIZE will do something like this {(Paragraph),(writing),(is)}
	flatten will convert list into row wise touples

	(to)
	(the)
	(whole.)
	(At)
	(Time4Writing)

*/

grouptokenz = GROUP toknz BY word;

/*

	(help,{(help)})
(more,{(more)})
(need,{(need)})
(than,{(than)})
(build,{(build)})

*/

occurence = FOREACH grouptokenz GENERATE group,COUNT(toknz) as c;

/*

(skills,1)
(whole.,1)
(element,1)
(nothing,1)
(process,1)
(teacher,1)
(whether,1)
(writing,7)
(elements,1)
(focusing,1)
(students,2)
(Paragraph,1)
(certified,1)
(creative.,1)
(essential,1)
(narrative,1)

*/

orderoccurence = ORDER occurence BY c DESC;

dump orderoccurence;
