

echo "Executing script..."


rm -rf build/
rm -rf dist/

python setup.py bdist_egg

cd dist/

echo "Done !!!"
