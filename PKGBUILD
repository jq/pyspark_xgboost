# Maintainer: Julian Qian <julian.qian@gmail.com>
pkgbase=python-pyspark-xgboost
pkgname=('python-pyspark-xgboost' 'python2-pyspark-xgboost')
_pyname=pyspark-xgboost
pkgver=0.1.0
pkgrel=1
pkgdesc='pyspark xgboost'
arch=('any')
url='https://github.com/jq/pyspark-xgboost'
license=('BSD')
makedepends=('python' 'python2' 'python-setuptools' 'python2-setuptools')
options=(!emptydirs)
source=("https://pypi.io/packages/source/${_pyname:0:1}/${_pyname}/${_pyname}-${pkgver}.tar.gz")
md5sums=('01189998819999197253aa0118999881')

prepare() {
  cd "${srcdir}/${_pyname}-${pkgver}"
  cp -r "${srcdir}/${_pyname}-${pkgver}" "${srcdir}/${_pyname}-${pkgver}-py2"
}

package_python-pyspark-xgboost() {
  depends=('python' 'python-setuptools')
  cd "${srcdir}/${_pyname}-${pkgver}"
  python3 setup.py install --root="${pkgdir}/" --optimize=1
  install -D -m644 LICENSE "${pkgdir}/usr/share/licenses/${pkgbase}/LICENSE"
}

package_python2-pyspark-xgboost() {
  depends=('python2' 'python2-setuptools')
  cd "${srcdir}/${_pyname}-${pkgver}-py2"
  python2 setup.py install --root="${pkgdir}/" --optimize=1
}

# vim:set ts=2 sw=2 et:
