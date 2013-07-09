import logging
import os
import re
import subprocess

from luigi import LocalTarget
import configuration
import hadoop
import hadoop_jar

logger = logging.getLogger('luigi-interface')

"""
Scalding support for Luigi.

Example configuration section in client.cfg:
[scalding]
# scala home directory, which should include a lib subdir with scala jars.
scala-home: /usr/share/scala

# scalding home directory, which should include a lib subdir with
# scalding-*-assembly-* jars as built from the official Twitter build script.
scalding-home: /usr/share/scalding

# provided dependencies, e.g. jars required for compiling but not executing
# scalding jobs. Currently requred jars:
# org.apache.hadoop/hadoop-core/0.20.2
# org.slf4j/slf4j-log4j12/1.6.6
# log4j/log4j/1.2.15
# commons-httpclient/commons-httpclient/3.1
# commons-cli/commons-cli/1.2
# org.apache.zookeeper/zookeeper/3.3.4
scalding-provided: /usr/share/scalding/provided

# additional jars required.
scalding-libjars: /usr/share/scalding/libjars
"""


class ScaldingJobRunner(hadoop.JobRunner):
    """JobRunner for `pyscald` commands. Used to run a ScaldingJobTask"""

    def __init__(self):
        conf = configuration.get_config()

        default = os.environ.get('SCALA_HOME', '/usr/share/scala')
        self.scala_home = conf.get('scalding', 'scala-home', default)

        default = os.environ.get('SCADLING_HOME', '/usr/share/scalding')
        self.scalding_home = conf.get('scalding', 'scalding-home', default)
        self.provided_dir = conf.get(
            'scalding', 'scalding-provided', os.path.join(default, 'provided'))
        self.libjars_dir = conf.get(
            'scalding', 'scalding-libjars', os.path.join(default, 'libjars'))

        self.tmp_dir = LocalTarget(is_tmp=True)

    def _get_jars(self, path):
        return [os.path.join(path, j) for j in os.listdir(path)
                if j.endswith('.jar')]

    def get_scala_jars(self, include_compiler=False):
        lib_dir = os.path.join(self.scala_home, 'lib')
        jars = [os.path.join(lib_dir, 'scala-library.jar')]

        # additional jar for scala 2.10 only
        reflect = os.path.join(lib_dir, 'scala-reflect.jar')
        if os.path.exists(reflect):
            jars.append(reflect)

        if include_compiler:
            jars.append(os.path.join(lib_dir, 'scala-compiler.jar'))

        return jars

    def get_scalding_jars(self):
        lib_dir = os.path.join(self.scalding_home, 'lib')
        return self._get_jars(lib_dir)

    def get_scalding_core(self):
        lib_dir = os.path.join(self.scalding_home, 'lib')
        for j in os.listdir(lib_dir):
            if j.startswith('scalding-core-'):
                p = os.path.join(lib_dir, j)
                logger.debug('Found scalding-core: %s', p)
                return p
        raise hadoop.HadoopJobError('Coudl not find scalding-core.')

    def get_provided_jars(self):
        return self._get_jars(self.provided_dir)

    def get_libjars(self):
        return self._get_jars(self.libjars_dir)

    def get_job_jar(self, source):
        job_name = os.path.basename(os.path.splitext(source)[0])
        return os.path.join(self.tmp_dir.path, job_name + '.jar')

    def get_build_dir(self, source):
        build_dir = os.path.join(self.tmp_dir.path, 'build')
        return build_dir

    def get_job_class(self, source):
        # find name of the job class
        # usually the one that matches file name or last class that extends Job
        job_name = os.path.splitext(os.path.basename(source))[0]
        job_class = None
        for l in open(source).readlines():
            p = re.search(r'class\s+([^\s\(]+).*extends\s+.*Job', l)
            if p:
                job_class = p.groups()[0]
                if job_class == job_name:
                    break
        if job_class:
            logger.debug('Found scalding job class: %s', job_class)
            return job_class
        else:
            raise hadoop.HadoopJobError('Coudl not find scalding job class.')

    def build_job_jar(self, job):
        job_src = job.source()
        job_jar = self.get_job_jar(job_src)
        if os.path.exists(job_jar):
            src_mtime = os.path.getmtime(job_src)
            jar_mtime = os.path.getmtime(job_jar)
            if jar_mtime > src_mtime:
                return job_jar

        build_dir = self.get_build_dir(job_src)
        if not os.path.exists(build_dir):
            os.makedirs(build_dir)

        classpath = ':'.join(filter(None,
                                    self.get_scalding_jars() +
                                    self.get_provided_jars() +
                                    self.get_libjars() +
                                    job.extra_jars()))
        scala_cp = ':'.join(self.get_scala_jars(include_compiler=True))

        # compile scala source
        arglist = ['java', '-cp', scala_cp, 'scala.tools.nsc.Main',
                   '-classpath', classpath,
                   '-d', build_dir, job_src]
        logger.info('Compiling scala source: %s', ' '.join(arglist))
        subprocess.check_call(arglist)

        # build job jar file
        arglist = ['jar', 'cf', job_jar, '-C', build_dir, '.']
        logger.info('Building job jar: %s', ' '.join(arglist))
        subprocess.check_call(arglist)
        return job_jar

    def run_job(self, job):
        if not job.source() or not os.path.exists(job.source()):
            logger.error("Can't find source: {0}, full path {1}".format(
                         job.source(), os.path.abspath(job.source())))
            raise Exception("job source does not exist")

        job_jar = self.build_job_jar(job)
        jars = [job_jar] + self.get_libjars() + job.extra_jars()
        scalding_core = self.get_scalding_core()
        libjars = ','.join(filter(None, jars))
        arglist = ['hadoop', 'jar', scalding_core, '-libjars', libjars]
        arglist += ['-D%s' % c for c in job.jobconfs()]

        job_class = job.job_class() or self.get_job_class(job.source())
        arglist += [job_class, '--hdfs']

        # scalding does not parse argument with '=' properly
        arglist += ['--name', job.task_id.replace('=', ':')]

        (tmp_files, job_args) = hadoop_jar.fix_paths(job)
        arglist += job_args

        env = os.environ.copy()
        jars.append(scalding_core)
        hadoop_cp = ':'.join(filter(None, jars))
        env['HADOOP_CLASSPATH'] = hadoop_cp
        logger.info("Submitting Hadoop job: HADOOP_CLASSPATH=%s %s",
                    hadoop_cp, ' '.join(arglist))
        hadoop.run_and_track_hadoop_job(arglist, env=env)

        for a, b in tmp_files:
            a.move(b)


class ScaldingJobTask(hadoop.BaseHadoopJobTask):
    """A job task for Scalding that define a scala source and (optional) main
    method

    requires() should return a dictionary where the keys are Scalding argument
    names and values are lists of paths. For example:
    {'input1': ['A', 'B'], 'input2': ['C']} => --input1 A B --input2 C
    """

    def relpath(self, current_file, rel_path):
        """Compute path given current file and relative path"""
        script_dir = os.path.dirname(os.path.abspath(current_file))
        rel_path = os.path.abspath(os.path.join(script_dir, rel_path))
        return rel_path

    def source(self):
        """Path to the scala source for this Scalding Job"""
        return None

    def extra_jars(self):
        """Extra jars for building and running this Scalding Job"""
        return []

    def job_class(self):
        """optional main job class for this Scalding Job"""
        return None

    def job_runner(self):
        return ScaldingJobRunner()

    def atomic_output(self):
        """If True, then rewrite output arguments to be temp locations and
        atomically move them into place after the job finishes"""
        return True

    def job_args(self):
        """Extra arguments to pass to the Scalding job"""
        return []

    def args(self):
        """returns an array of args to pass to the job."""
        arglist = []
        for k, v in self.requires_hadoop().iteritems():
            arglist.append('--' + k)
            arglist.extend([t.output().path for t in v])
        arglist.extend(['--output', self.output()])
        arglist.extend(self.job_args())
        return arglist
