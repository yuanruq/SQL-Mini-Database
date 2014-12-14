/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2014 Oracle and/or its affiliates. All rights reserved.
 *
 */

import java.util.Map;

import com.sun.javadoc.AnnotationDesc;
import com.sun.javadoc.AnnotationTypeDoc;
import com.sun.javadoc.ClassDoc;
import com.sun.javadoc.PackageDoc;
import com.sun.javadoc.SeeTag;
import com.sun.javadoc.SourcePosition;
import com.sun.javadoc.Tag;

class HidingPackageDocWrapper extends HidingWrapper implements PackageDoc {
    public HidingPackageDocWrapper(PackageDoc packdoc, Map mapWrappers) {
        super(packdoc, mapWrappers);
    }

    private PackageDoc _getPackageDoc() {
        return (PackageDoc)getWrappedObject();
    }

    public ClassDoc[] allClasses() {
        return (ClassDoc[])wrapOrHide(_getPackageDoc().allClasses());
    }

    public ClassDoc[] allClasses(boolean filter) {
        return (ClassDoc[])wrapOrHide(_getPackageDoc().allClasses(filter));
    }

    public ClassDoc[] errors() {
        return (ClassDoc[])wrapOrHide(_getPackageDoc().errors());
    }

    public ClassDoc[] exceptions() {
        return (ClassDoc[])wrapOrHide(_getPackageDoc().exceptions());
    }

    public ClassDoc findClass(String szClassName) {
        return (ClassDoc)wrapOrHide(_getPackageDoc().findClass(szClassName));
    }

    public ClassDoc[] interfaces() {
        return (ClassDoc[])wrapOrHide(_getPackageDoc().interfaces());
    }

    public ClassDoc[] ordinaryClasses() {
        return (ClassDoc[])wrapOrHide(_getPackageDoc().ordinaryClasses());
    }

    public AnnotationDesc[] annotations() {
        return _getPackageDoc().annotations();
    }

    public AnnotationTypeDoc[] annotationTypes() {
        return _getPackageDoc().annotationTypes();
    }

    public ClassDoc[] enums() {
        return (ClassDoc[])wrapOrHide(_getPackageDoc().enums());
    }
  
    public boolean isEnumConstant() {
        return _getPackageDoc().isEnumConstant();
    }
  
    public String commentText() {
        return _getPackageDoc().commentText();
    }

    public int compareTo(Object obj) {
        if (obj instanceof HidingWrapper) {
            return _getPackageDoc().
                   compareTo(((HidingWrapper)obj).getWrappedObject());
        } else {
            return _getPackageDoc().compareTo(obj);
        }
    }

    public Tag[] firstSentenceTags() {
        return (Tag[])wrapOrHide(_getPackageDoc().firstSentenceTags());
    }

    public String getRawCommentText() {
        return _getPackageDoc().getRawCommentText();
    }

    public Tag[] inlineTags() {
        return (Tag[])wrapOrHide(_getPackageDoc().inlineTags());
    }

    public boolean isClass() {
        return _getPackageDoc().isClass();
    }

    public boolean isConstructor() {
        return _getPackageDoc().isConstructor();
    }

    public boolean isError() {
        return _getPackageDoc().isError();
    }

    public boolean isException() {
        return _getPackageDoc().isException();
    }

    public boolean isField() {
        return _getPackageDoc().isField();
    }

    public boolean isIncluded() {
        return _getPackageDoc().isIncluded();
    }

    public boolean isInterface() {
        return _getPackageDoc().isInterface();
    }

    public boolean isMethod() {
        return _getPackageDoc().isMethod();
    }

    public boolean isOrdinaryClass() {
        return _getPackageDoc().isOrdinaryClass();
    }

    public String name() {
        return _getPackageDoc().name();
    }

    public SeeTag[] seeTags() {
        return (SeeTag[])wrapOrHide(_getPackageDoc().seeTags());
    }

    public void setRawCommentText(String szText) {
        _getPackageDoc().setRawCommentText(szText);
    }

    public Tag[] tags() {
        return (Tag[])wrapOrHide(_getPackageDoc().tags());
    }

    public Tag[] tags(String szTagName) {
        return (Tag[])wrapOrHide(_getPackageDoc().tags(szTagName));
    }
 
    public SourcePosition position() {
        return _getPackageDoc().position();
    }

    public boolean isAnnotationType() {
        return _getPackageDoc().isAnnotationType();
    }

    public boolean isEnum() {
        return _getPackageDoc().isEnum();
    }

    public boolean isAnnotationTypeElement() {
        return _getPackageDoc().isAnnotationTypeElement();
    }
}
