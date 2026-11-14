"""Keep workload/analysis dependencies outside the hardware models."""
import ast
from pathlib import Path
import subprocess
import sys
import unittest


ROOT = Path(__file__).resolve().parents[1]
PACKAGE = ROOT


class ArchitectureTests(unittest.TestCase):
    def test_model_and_runtime_import_boundaries(self):
        # Check imports inside functions as well as module-level imports.
        # New models may share message contracts, but must not import the driver,
        # capture loader, workloads or presentation code.
        for folder in ('models', 'runtime'):
            for path in (PACKAGE / folder).rglob('*.py'):
                for node in ast.walk(ast.parse(path.read_text())):
                    if isinstance(node, ast.ImportFrom):
                        self.assertEqual(node.level, 0, str(path))
                        imports = [node.module]
                    elif isinstance(node, ast.Import):
                        imports = [alias.name for alias in node.names]
                    else:
                        continue
                    for name in imports:
                        with self.subTest(file=str(path.relative_to(ROOT)), imported=name):
                            section = name.split('.')[0]
                            if section not in ('models', 'runtime', 'frontend', 'workloads', 'analysis',
                                               'api', 'cli', 'simulator', 'config', 'errors', 'records', 'tiles'):
                                self.assertFalse(name.startswith(('numpy', 'matplotlib', 'ssnpu_model', 'wse_perf_sim')))
                                continue
                            self.assertIn(section, ('models', 'runtime', 'config', 'errors', 'records', 'tiles'))
                            if folder == 'runtime':
                                self.assertNotEqual(section, 'models')
                            if path.is_relative_to(PACKAGE / 'models/noc'):
                                self.assertFalse(name.startswith(('models.ssnpu',
                                                                  'models.memory')))
                            if path.is_relative_to(PACKAGE / 'models/ssnpu') and name.startswith('models.noc'):
                                self.fail('Core imports NoC implementation: ' + name)

    def test_noc_import_does_not_load_core_or_capture(self):
        # A fresh standard-library-only process catches transitive __init__ imports.
        code = '''
import sys
from models.noc.unit import CommUnit
from models.noc.compiler import compile_phase
for name in sys.modules:
    assert not name.startswith(('models.ssnpu', 'frontend',
                               'workloads', 'analysis',
                               'numpy', 'matplotlib', 'ssnpu_model')), name
'''
        subprocess.run([sys.executable, '-S', '-c', code], cwd=ROOT, check=True,
                       capture_output=True, text=True)

    def test_public_api_does_not_load_optional_workloads(self):
        code = '''
import sys
from api import simulate
assert callable(simulate)
assert not any(name.startswith(('numpy', 'matplotlib', 'workloads'))
               for name in sys.modules)
'''
        subprocess.run([sys.executable, '-S', '-c', code], cwd=ROOT, check=True,
                       capture_output=True, text=True)
