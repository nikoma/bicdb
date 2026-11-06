#!/usr/bin/env python3
"""Inventory direct JUnit test annotations in a pinned upstream JAR without execution.

This is candidate discovery, not a JUnit execution plan or compatibility result.
Inherited/composed annotations, runtime conditions and invocation expansion need
actual framework discovery and execution. Raw disassembly is not retained.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import zipfile

TEST_ANNOTATIONS = {
    'org.junit.Test', 'org.junit.jupiter.api.Test',
    'org.junit.jupiter.params.ParameterizedTest', 'org.junit.jupiter.api.RepeatedTest',
    'org.junit.jupiter.api.TestFactory', 'org.junit.jupiter.api.TestTemplate',
}
ANNOTATION = re.compile(r'^\s+(org\.junit\.[\w.$]+)\s*(?:\(|$)',re.M)
HEADER = re.compile(r'^  \S[^\n]*\([^\n]*\)(?: throws [^\n;]+)?;$',re.M)


def parse_disassembly(text):
    classes=[]
    seen=set()
    for block in re.split(r'(?m)^Classfile ',text)[1:]:
        identity=re.search(r'^  this_class:\s+#\d+\s+// (\S+)$',block,re.M)
        if identity is None:raise ValueError('disassembly has no class identity')
        name=identity.group(1).replace('/','.')
        if name in seen:raise ValueError('duplicate class identity')
        seen.add(name)
        begin=re.search(r'^\{$',block,re.M)
        end=re.search(r'^\}$',block,re.M)
        if begin is None or end is None or end.start()<begin.end():
            raise ValueError('disassembly has no complete class body')
        body=block[begin.end():end.start()]
        methods=[]
        headers=list(HEADER.finditer(body))
        for index,header in enumerate(headers):
            section=body[header.end():headers[index+1].start() if index+1<len(headers) else len(body)]
            annotations=sorted(set(ANNOTATION.findall(section)))
            tests=sorted(set(annotations)&TEST_ANNOTATIONS)
            if not tests:continue
            descriptor=re.search(r'^    descriptor: (\S+)$',section,re.M)
            if descriptor is None:raise ValueError('annotated method has no descriptor')
            method=header.group(0).split('(',1)[0].split()[-1]
            methods.append(dict(name=method,descriptor=descriptor.group(1),test_annotations=tests,
                junit_annotations=annotations,disabled_annotation='org.junit.jupiter.api.Disabled' in annotations or 'org.junit.Ignore' in annotations))
        flags=re.search(r'^  flags: (.*)$',block,re.M)
        if flags is None:raise ValueError('class access flags are missing')
        class_annotations=sorted(set(ANNOTATION.findall(block[end.end():])))
        classes.append(dict(name=name,abstract='ACC_ABSTRACT' in flags.group(1),
            class_junit_annotations=class_annotations,
            disabled_annotation='org.junit.jupiter.api.Disabled' in class_annotations or 'org.junit.Ignore' in class_annotations,
            direct_test_methods=methods))
    if not classes:raise ValueError('no class disassembly found')
    return classes


def inventory(jar,prefix,*,javap='javap',expected_sha256=None):
    if not prefix or not jar.is_file():raise ValueError('a JAR and nonempty class prefix are required')
    with tempfile.TemporaryDirectory(prefix='bicdb-upstream-test-metadata-') as temporary:
        snapshot=Path(temporary)/'pinned.jar'
        shutil.copyfile(jar,snapshot)
        digest=hashlib.sha256(snapshot.read_bytes()).hexdigest()
        if expected_sha256 is not None and digest!=expected_sha256.lower():
            raise ValueError('JAR snapshot differs from its expected SHA-256')
        with zipfile.ZipFile(snapshot) as archive:
            entries=[name for name in archive.namelist() if name.endswith('.class') and name[:-6].replace('/','.').startswith(prefix)]
            names=sorted(name[:-6].replace('/','.') for name in entries)
            if not names or len(set(names))!=len(names):raise ValueError('class inventory is empty or duplicated')
            digests={name[:-6].replace('/','.'):hashlib.sha256(archive.read(name)).hexdigest() for name in entries}
        output=Path(temporary)/'javap.txt'
        with output.open('wb') as stream:
            subprocess.run([javap,'-J-Xmx256m','-v','-p','-classpath',str(snapshot),*names],stdout=stream,stderr=subprocess.PIPE,timeout=120,check=True)
        classes=parse_disassembly(output.read_text())
    if {item['name'] for item in classes}!=set(names):raise ValueError('disassembly did not cover the exact JAR inventory')
    annotated=[]
    for item in classes:
        item['class_sha256']=digests[item['name']]
        if item['direct_test_methods']:annotated.append(item)
    annotated.sort(key=lambda item:item['name'])
    return dict(format=1,jar_sha256=digest,class_prefix=prefix,
        examined_classes=len(classes),classes_with_direct_tests=len(annotated),
        direct_test_methods=sum(len(item['direct_test_methods']) for item in annotated),classes=annotated,
        source_code_modified=False,application_executed=False,execution_plan_qualified=False,
        runtime_conditions_evaluated=False,application_qualified=False)


def validate_report_path(jar,report):
    if jar.resolve()==report.resolve() or (report.exists() and jar.samefile(report)):
        raise ValueError('report must not overwrite the input JAR')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--jar',type=Path,required=True)
    parser.add_argument('--prefix',default='org.idempiere.test.')
    parser.add_argument('--javap',default='javap')
    parser.add_argument('--expected-sha256')
    parser.add_argument('--report',type=Path,required=True)
    args=parser.parse_args()
    try:validate_report_path(args.jar,args.report)
    except ValueError as error:parser.error(str(error))
    result=inventory(args.jar,args.prefix,javap=args.javap,expected_sha256=args.expected_sha256)
    args.report.write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps({key:result[key] for key in ['examined_classes','classes_with_direct_tests','direct_test_methods','application_qualified']}))


if __name__=='__main__':main()
